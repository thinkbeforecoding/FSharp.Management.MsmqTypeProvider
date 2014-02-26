namespace FSharp.Management

open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System.Messaging

type Queue(name, path) =
    member x.Name = name
    member x.Path = path
    member x.Purge() = 
        use queue = new MessageQueue(path)
        queue.Purge()
    member x.Messages =
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