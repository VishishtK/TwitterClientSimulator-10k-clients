#r "nuget: Akka.Fsharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic
open System.Text.RegularExpressions

type Message = {
    Action: String;
    User: String;
    Tweet: String;
    TweetId: String;
    ReTweetId: String;
    Mentions: String;
    HashTags: String;
    ToSubscribe: String
    Online: Boolean
}

type Response = {
    Action: String;
    Message : String;
    Success : String
}

let defaultMessage = {Action=" "; User=" "; Tweet=" "; TweetId=" "; ReTweetId=" "; Mentions= "false"; HashTags=" "; ToSubscribe=" "; Online = true}
let defaultResponse = {Action=" "; Message=" "; Success= "true"}

let config = 
      ConfigurationFactory.ParseString(
        @"akka {
            actor.serializers{
              json  = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
              bytes = ""Akka.Serialization.ByteArraySerializer""
            }
             actor.serialization-bindings {
              ""System.Byte[]"" = bytes
              ""System.Object"" = json
            
            }
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote.helios.tcp {
                hostname = ""localhost""
                port = 9001
            }
        }")

let system = ActorSystem.Create("twitterServer",config)

let users = new Dictionary<string, IActorRef>()
let onlineStatus = new Dictionary<string,Boolean>()
let userTweets = new Dictionary<string, List<Message>>()
let tweets = new Dictionary<string, Message>()
let subscriptions = new Dictionary<string, List<String>>()
let subscribers = new Dictionary<string, List<String>>()
let hashTagMapping = new Dictionary<string, List<Message>>()
let mentionMap = new Dictionary<string, List<Message>>()
let feed = new Dictionary<string,List<String>>()

let getActorFromUserName userName= 
    let actorPath = "akka.tcp://Twitter@" + "localhost" + ":9002/user/" + userName
    select actorPath system

let processHashTag tweet hashTag = 
    let containsHashTag,list = hashTagMapping.TryGetValue(hashTag)
    if containsHashTag
    then
        list.Add(tweet)
    else
        let newList = List [tweet]
        hashTagMapping.Add(hashTag,newList)

let processMention tweet mention = 
    let containsMention,list = mentionMap.TryGetValue(mention)
    if containsMention
    then
        list.Add(tweet)

let processTweet tweet =
    let containsUser, list = userTweets.TryGetValue(tweet.User)
    if containsUser
    then
        list.Add(tweet)
        let words = tweet.Tweet.Split [|' '|]
        for word in words do
            if Regex.IsMatch(word, "^(@)([a-zA-Z])+")
            then
                processMention tweet word
            else if Regex.IsMatch(word, "^(#)([a-zA-Z0-9])+")
            then
                processHashTag tweet word
        true           
    else
        false

let publishTweet tweet = 
    let subsribersExist, subscribersList = subscribers.TryGetValue(tweet.User)
    let mutable publishCount = 0
    if subsribersExist then
        for subscriber in subscribersList do
            let feedExists, userFeed = feed.TryGetValue(subscriber)
            userFeed.Add(tweet.TweetId)
            publishCount <- publishCount + 1
            let userExists, online = onlineStatus.TryGetValue(subscriber)
            if online then
                getActorFromUserName subscriber <! {defaultResponse with Action = "Tweet"; Message = tweet.Tweet}
    publishCount        

let twitterServer(mailbox: Actor<_>) =
    let rec loop tweetID tweetCount tweetPublishCount ()= actor{
        let! (msg:Message) = mailbox.Receive();
        let sender = mailbox.Sender()
        let self = mailbox.Self
        match msg.Action with
        | "RegisterAccount" ->
            if not(users.ContainsKey(msg.User))
            then 
                users.Add(msg.User, sender)     
                userTweets.Add(msg.User,new List<Message>())
                mentionMap.Add(msg.User,new List<Message>())
                feed.Add(msg.User,new List<String>())
                subscribers.Add(msg.User,new List<String>())
                subscriptions.Add(msg.User,new List<String>())
                sender <! {defaultResponse with Action = "RegisterAccount"; Message = msg.User + " Registered"}
        | "Tweet" ->
            // Add live delivering of tweets
            // Add to the feed
            // Fix twitterID
            // Send notification to mentioned user
            if processTweet msg
            then
                tweets.Add(msg.User+"-"+(string)tweetID,msg)
                let publishCount = publishTweet msg
                sender <! {defaultResponse with Action = "Tweet"; Message = "Tweeted the message"}
                return! loop (tweetID+1) (tweetCount+1) (tweetPublishCount+publishCount) ()
            else
                sender <! {defaultResponse with Action = "Tweet"; Message = "Some Error Occured"; Success = "false"}
            
        | "ReTweet" ->
            let tweetExists, tweet = tweets.TryGetValue(msg.TweetId)
            if tweetExists then
                self <! {tweet with User = msg.User}
            else
                sender <! {defaultResponse with Action = "ReTweet"; Message = "Tweet does not exist"; Success = "false"}
                
        | "Subscribe" ->
            let subscriberExist, subscriber = users.TryGetValue(msg.User)
            let publisherExist, publisher = users.TryGetValue(msg.ToSubscribe)
            if subscriberExist&&publisherExist then
                let publisherExist, publishersList = subscribers.TryGetValue(msg.ToSubscribe)
                let subscriptionsExist, subscriptionsList = subscriptions.TryGetValue(msg.User)
                publishersList.Add(msg.User)
                subscriptionsList.Add(msg.ToSubscribe)
        | "Query" ->
            let mutable message = ""
            if msg.Mentions = "true"
            then
                let mentionsExist, mentionsList = mentionMap.TryGetValue(msg.User)
                for tweet in mentionsList do
                    message <- "TweetID:"+tweet.TweetId+" Tweet: "+ tweet.Tweet + "\n"
            if msg.HashTags.Length = 0 then
                let hashTagExists, hashTagList = hashTagMapping.TryGetValue(msg.HashTags)
                for tweet in hashTagList do
                    message <- "TweetID:"+tweet.TweetId+" Tweet: "+ tweet.Tweet + "\n"
            sender <! {defaultResponse with Action = "Query"; Message = message}        
        | "Feed" ->
            let feedExists, userFeed = feed.TryGetValue(msg.User)
            if feedExists
            then
                let mutable message = ""
                for tweet in userFeed do
                    message <- "TweetID:"+ tweet + "\n"
                sender <! {defaultResponse with Action = "Feed"; Message = message}
            else
                sender <! {defaultResponse with Action = "Tweet"; Message = "Feed does not exist"; Success = "false"}
        | "Online" ->
            onlineStatus.Remove(msg.User) |> ignore
            onlineStatus.Add(msg.User,msg.Online)
            if msg.Online then
                let feedExists, userFeed = feed.TryGetValue(msg.User)
                if feedExists
                then
                    let mutable message = ""
                    for tweet in userFeed do
                        message <- "TweetID:"+ tweet + "\n"
                    sender <! {defaultResponse with Action = "Feed"; Message = message}
                    return! loop tweetID tweetCount (tweetPublishCount + userFeed.Count) ()
                else
                    sender <! {defaultResponse with Action = "Tweet"; Message = "Feed does not exist"; Success = "false"}
        | _ -> 
            printfn "Tweets Recieved Per Second : %i" tweetCount
            printfn "Tweets Published Per Second : %i \n" tweetPublishCount
            return! loop tweetID 0 0 ()

        return! loop tweetID tweetCount tweetPublishCount ()
    }
    loop 0 0 0()

let twitterServerRef = spawn system "twitterServer" twitterServer
system.Scheduler.ScheduleTellRepeatedly(
    TimeSpan.FromSeconds(1.0),
    TimeSpan.FromSeconds(1.0),
    twitterServerRef,
    {defaultMessage with Action = "Print";}
)
Console.ReadLine() |> ignore