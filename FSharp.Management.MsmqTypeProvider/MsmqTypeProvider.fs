namespace FSharp.Management

open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System
open System.Messaging

type Queue(name, path) =
    let observers =  Collections.Generic.List<IObserver<_>>()
    
    let fetcher = async {
        use queue = new MessageQueue(path)
        use cursor = queue.CreateCursor()
        let rec loop action =
            async {
            try
                let! message = Async.FromBeginEnd((fun (c,s) -> queue.BeginPeek(TimeSpan.FromSeconds 10., cursor, action, s, c)), queue.EndPeek) 
                observers |> Seq.iter (fun o -> o.OnNext(message))
                message.Dispose()
                do! loop PeekAction.Next
            with
            | :? MessageQueueException as ex when ex.ErrorCode = -2147467259 ->
                    do! loop PeekAction.Current }
        do! loop PeekAction.Current
    }
    let c = new Threading.CancellationTokenSource()

    member x.Name = name
    member x.Path = path
    member x.Purge() = 
        use queue = new MessageQueue(path)
        queue.Purge()

    member x.Transactional =
        use queue = new MessageQueue(path)
        queue.Transactional

    member x.GetMessages() =
        seq {
        use queue = new MessageQueue(path)
        use e = queue.GetMessageEnumerator2()
        while e.MoveNext() do
            yield e.Current
            e.Current.Dispose() }

    member x.Send(message, ?label, ?transaction) =
        use queue = new MessageQueue(path)
        match transaction with
        | Some transaction -> queue.Send(message, defaultArg label "", (transaction: MessageQueueTransaction))
        | None -> 
            let tx = if queue.Transactional then MessageQueueTransactionType.Single else MessageQueueTransactionType.None
            queue.Send(message, defaultArg label "", tx)
      
    member x.Send(message: Message, ?transaction) =
        use queue = new MessageQueue(path)
        match transaction with
        | Some transaction -> queue.Send(message,  (transaction: MessageQueueTransaction))
        | None -> 
            let tx = if queue.Transactional then MessageQueueTransactionType.Single else MessageQueueTransactionType.None
            queue.Send(message, tx)
        
    member x.Messages =

        { new System.IObservable<Message> with
            member x.Subscribe(observer) =
                observers.Add observer
                if observers.Count = 1 then
                    Async.Start(fetcher, c.Token)
                { new IDisposable with
                    member x.Dispose() = 
                        observers.Remove observer |> ignore 
                        if observers.Count = 0 then
                            c.Cancel() } }
[<TypeProvider>]
type MsmqTypeProvider() as this =
    inherit TypeProviderForNamespaces()
    let asm = System.Reflection.Assembly.GetExecutingAssembly()

    let priv = 
        let p = ProvidedTypeDefinition(asm, "Msmq", "Private", Some typeof<obj>)

        let members() =
            let queues = MessageQueue.GetPrivateQueuesByMachine(".")
            try
                [ for queue in queues ->
                   let name = queue.QueueName.Substring(9)
                   let path = queue.Path
                   let prop = ProvidedProperty(name, typeof<Queue>)
                   prop.GetterCode <- fun _ -> <@@ Queue(name, path) @@>
                   prop.IsStatic <- true
                   prop
                   ]
            finally
                queues |> Array.iter (fun q -> q.Dispose())

        p.AddMembersDelayed(members)
        p

    do this.AddNamespace("Msmq", [priv])

[<assembly:TypeProviderAssembly>]
do()