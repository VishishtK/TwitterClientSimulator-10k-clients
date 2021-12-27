#r "nuget: Akka.Fsharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Threading


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
                port = 9002
            }
        }")

let system = ActorSystem.Create("Twitter",config)
let twitterServer = system.ActorSelection ("akka.tcp://twitterServer@" + "localhost" + ":9001/user/twitterServer")

let randomMentionsCount = [0;0;0;0;0;0;1;1;1;2;2;3]
let randomSentenceLength = [1..10]
let randomHashTagsCount = [0;0;0;0;1;1;1;2;2;3]
let words = [|
    "ability";"able";"about";"above";"accept";"according";"account";"across";"act";"action";"activity";"actually";"add";"address";"administration";"admit";"adult";"affect";"after";"again";"against";"age";"agency";"agent";"ago";"agree";"agreement";"ahead";"air";"all";"allow";"almost";"alone";"along";"already";"also";"although";"always";"American";"among";"amount";"analysis";"animal";"another";"answer";"any";"anyone";"anything";"appear";"apply";"approach";"area";"argue";"arm";"around";"arrive";"art";"article";"artist";"as";"ask";"assume";"at";"attack";"attention";"attorney";"audience";"author";"authority";"available";"avoid";"away";"baby";"back";"bad";"bag";"ball";"bank";"bar";"base";"be";"beat";"beautiful";"because";"become";"bed";"before";"begin";"behavior";"behind";"believe";"benefit";"best";"better";"between";"beyond";"big";"bill";"billion";"bit";"black";"blood";"blue";"board";"body";"book";"born";"both";"box";"boy";"break";"bring";"brother";"budget";"build";"building";"business";"but";"buy";"by";"call";"camera";"campaign";"can";"cancer";"candidate";"capital";"car";"card";"care";"career";"carry";"case";"catch";"cause";"cell";"center";"central";"century";"certain";"certainly";"chair";"challenge";"chance";"change";"character";"charge";"check";"child";"choice";"choose";"church";"citizen";"city";"civil";"claim";"class";"clear";"clearly";"close";"coach";"cold";"collection";"college";"color";"come";"commercial";"common";"community";"company";"compare";"computer";"concern";"condition";"conference";"Congress";"consider";"consumer";"contain";"continue";"control";"cost";"could";"country";"couple";"course";"court";"cover";"create";"crime";"cultural";"culture";"cup";"current";"customer";"cut";"dark";"data";"daughter";"day";"dead";"deal";"death";"debate";"decade";"decide";"decision";"deep";"defense";"degree";"Democrat";"democratic";"describe";"design";"despite";"detail";"determine";"develop";"development";"die";"difference";"different";"difficult";"dinner";"direction";"director";"discover";"discuss";"discussion";"disease";"do";"doctor";"dog";"door";"down";"draw";"dream";"drive";"drop";"drug";"during";"each";"early";"east";"easy";"eat";"economic";"economy";"edge";"education";"effect";"effort";"eight";"either";"election";"else";"employee";"end";"energy";"enjoy";"enough";"enter";"entire";"environment";"environmental";"especially";"establish";"even";"evening";"event";"ever";"every";"everybody";"everyone";"everything";"evidence";"exactly";"example";"executive";"exist";"expect";"experience";"expert";"explain";"eye";"face";"fact";"factor";"fail";"fall";"family";"far";"fast";"father";"fear";"federal";"feel";"feeling";"few";"field";"fight";"figure";"fill";"film";"final";"finally";"financial";"find";"fine";"finger";"finish";"fire";"firm";"first";"fish";"five";"floor";"fly";"focus";"follow";"food";"foot";"for";"force";"foreign";"forget";"form";"former";"forward";"four";"free";"friend";"from";"front";"full";"fund";"future";"game";"garden";"gas";"general";"generation";"get";"girl";"give";"glass";"go";"goal";"good";"government";"great";"green";"ground";"group";"grow";"growth";"guess";"gun";"guy";"hair";"half";"hand";"hang";"happen";"happy";"hard";"have";"he";"head";"health";"hear";"heart";"heat";"heavy";"help";"her";"here";"herself";"high";"him";"himself";"his";"history";"hit";"hold";"home";"hope";"hospital";"hot";"hotel";"hour";"house";"how";"however";"huge";"human";"hundred";"husband";"I";"idea";"identify";"if";"image";"imagine";"impact";"important";"improve";"in";"include";"including";"increase";"indeed";"indicate";"individual";"industry";"information";"inside";"instead";"institution";"interest";"interesting";"international";"interview";"into";"investment";"involve";"issue";"it";"item";"its";"itself";"job";"join";"just";"keep";"key";"kid";"kill";"kind";"kitchen";"know";"knowledge";"land";"language";"large";"last";"late";"later";"laugh";"law";"lawyer";"lay";"lead";"leader";"learn";"least";"leave";"left";"leg";"legal";"less";"let";"letter";"level";"lie";"life";"light";"like";"likely";"line";"list";"listen";"little";"live";"local";"long";"look";"lose";"loss";"lot";"love";"low";"machine";"magazine";"main";"maintain";"major";"majority";"make";"man";"manage";"management";"manager";"many";"market";"marriage";"material";"matter";"may";"maybe";"me";"mean";"measure";"media";"medical";"meet";"meeting";"member";"memory";"mention";"message";"method";"middle";"might";"military";"million";"mind";"minute";"miss";"mission";"model";"modern";"moment";"money";"month";"more";"morning";"most";"mother";"mouth";"move";"movement";"movie";"Mr";"Mrs";"much";"music";"must";"my";"myself";"name";"nation";"national";"natural";"nature";"near";"nearly";"necessary";"need";"network";"never";"new";"news";"newspaper";"next";"nice";"night";"no";"none";"nor";"north";"not";"note";"nothing";"notice";"now";"n't";"number";"occur";"of";"off";"offer";"office";"officer";"official";"often";"oh";"oil";"ok";"old";"on";"once";"one";"only";"onto";"open";"operation";"opportunity";"option";"or";"order";"organization";"other";"others";"our";"out";"outside";"over";"own";"owner";"page";"pain";"painting";"paper";"parent";"part";"participant";"particular";"particularly";"partner";"party";"pass";"past";"patient";"pattern";"pay";"peace";"people";"per";"perform";"performance";"perhaps";"period";"person";"personal";"phone";"physical";"pick";"picture";"piece";"place";"plan";"plant";"play";"player";"PM";"point";"police";"policy";"political";"politics";"poor";"popular";"population";"position";"positive";"possible";"power";"practice";"prepare";"present";"president";"pressure";"pretty";"prevent";"price";"private";"probably";"problem";"process";"produce";"product";"production";"professional";"professor";"program";"project";"property";"protect";"prove";"provide";"public";"pull";"purpose";"push";"put";"quality";"question";"quickly";"quite";"race";"radio";"raise";"range";"rate";"rather";"reach";"read";"ready";"real";"reality";"realize";"really";"reason";"receive";"recent";"recently";"recognize";"record";"red";"reduce";"reflect";"region";"relate";"relationship";"religious";"remain";"remember";"remove";"report";"represent";"Republican";"require";"research";"resource";"respond";"response";"responsibility";"rest";"result";"return";"reveal";"rich";"right";"rise";"risk";"road";"rock";"role";"room";"rule";"run";"safe";"same";"save";"say";"scene";"school";"science";"scientist";"score";"sea";"season";"seat";"second";"section";"security";"see";"seek";"seem";"sell";"send";"senior";"sense";"series";"serious";"serve";"service";"set";"seven";"several";"sex";"sexual";"shake";"share";"she";"shoot";"short";"shot";"should";"shoulder";"show";"side";"sign";"significant";"similar";"simple";"simply";"since";"sing";"single";"sister";"sit";"site";"situation";"six";"size";"skill";"skin";"small";"smile";"so";"social";"society";"soldier";"some";"somebody";"someone";"something";"sometimes";"son";"song";"soon";"sort";"sound";"source";"south";"southern";"space";"speak";"special";"specific";"speech";"spend";"sport";"spring";"staff";"stage";"stand";"standard";"star";"start";"state";"statement";"station";"stay";"step";"still";"stock";"stop";"store";"story";"strategy";"street";"strong";"structure";"student";"study";"stuff";"style";"subject";"success";"successful";"such";"suddenly";"suffer";"suggest";"summer";"support";"sure";"surface";"system";"table";"take";"talk";"task";"tax";"teach";"teacher";"team";"technology";"television";"tell";"ten";"tend";"term";"test";"than";"thank";"that";"the";"their";"them";"themselves";"then";"theory";"there";"these";"they";"thing";"think";"third";"this";"those";"though";"thought";"thousand";"threat";"three";"through";"throughout";"throw";"thus";"time";"to";"today";"together";"tonight";"too";"top";"total";"tough";"toward";"town";"trade";"traditional";"training";"travel";"treat";"treatment";"tree";"trial";"trip";"trouble";"true";"truth";"try";"turn";"TV";"two";"type";"under";"understand";"unit";"until";"up";"upon";"us";"use";"usually";"value";"various";"very";"victim";"view";"violence";"visit";"voice";"vote";"wait";"walk";"wall";"want";"war";"watch";"water";"way";"we";"weapon";"wear";"week";"weight";"well";"west";"western";"what";"whatever";"when";"where";"whether";"which";"while";"white";"who";"whole";"whom";"whose";"why";"wide";"wife";"will";"win";"wind";"window";"wish";"with";"within";"without";"woman";"wonder";"word";"work";"worker";"world";"worry";"would";"write";"writer";"wrong";"yard";"yeah";"year";"yes";"yet";"you";"young";"your";"yourself"
    |] 
let wordsList = words |> Array.toList

let noOfClients = 100  // EDIT TO CHANGE NO OF CLIENTS
let randomSubcribptionCount = [1..100]

let ranStr n = 
    let r = Random()
    let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
    let sz = Array.length chars in
    String(Array.init n (fun _ -> chars.[r.Next sz]))

let pickRandom (l: List<_>) =
    let r = Random()
    l.[r.Next(l.Length)]

let sentence() = 
  let mutable temp = ""
  let length = pickRandom randomSentenceLength
  for i in 1..length do
    temp <- temp + (pickRandom wordsList) + " "
  temp

let mentions() =
  let mutable temp = ""
  let noOfMentions = pickRandom randomMentionsCount
  if noOfMentions <> 0 then
    for i in 1..noOfMentions do
      temp <- temp + "@"+"twitterClient" + (string)(pickRandom [1..noOfClients]) + " "
  temp

let hashtags() = 
  let mutable temp = ""
  let noOfHashTags = pickRandom randomHashTagsCount
  if noOfHashTags <> 0 then
    for i in 1..noOfHashTags do
      temp <- temp + "#" + (pickRandom wordsList) + " "
  temp

// Generate random tweets with random hashtags and mentions
let tweet() = 
  sentence() + mentions() + hashtags()

let simulatorInterface (mailbox: Actor<_>) = 
  let rec loop () = actor{
    let! msg = mailbox.Receive();
    printfn "%s" msg
    return! loop ()
  }
  loop ()

let simulatorInterfaceRef = spawn system "simulatorInterface" simulatorInterface

let twitterClient user (mailbox: Actor<_>) =
    let rec loop feed online ()= actor{
        let! (msg:obj) = mailbox.Receive();
        let sender = mailbox.Sender()
        match msg with
        | :? Message as msg ->
          match msg.Action with
          // Merge into one
          | "RegisterAccount" -> if online then twitterServer <! {defaultMessage with User = user; Action = "RegisterAccount"}
          | "Tweet" -> if online then twitterServer <! {defaultMessage with User = user; Tweet = tweet(); Action = "Tweet"}
          | "ReTweet" ->if online then twitterServer <! {msg with User = user}
          | "Subscribe" ->if online then twitterServer<! {msg with User = user}
          | "Query" ->if online then twitterServer <! {msg with User = user}
          | "Feed" -> if online then twitterServer <! {msg with User = user}
          | "Online" -> 
              let status = pickRandom [true;false]
              if status <> online then
                twitterServer <! {msg with User = user}
                return! loop [||] status ()
          | _ -> ()
        | :? Response as msg ->
          match msg.Success with
          | "true" ->
            // simulatorInterfaceRef <! "Success : " + msg.Message
            if msg.Action = "Tweet" then
              return! loop (Array.append feed [|msg|]) online ()
            elif msg.Action = "Feed" then
              return! loop (Array.append feed [|msg|]) online ()
          // | "false" ->
          //   simulatorInterfaceRef <! "Fail : " + msg.Message
          | _ ->()
        |_->()
        return! loop feed online ()
    }
    loop [||] true ()

// Spawn clients
let clients = [
  for clientNo = 1 to noOfClients do
    spawn system ("twitterClient"+(string)clientNo) (twitterClient ("twitterClient"+(string)clientNo))
]

// Register with twitter
for client in clients do
  client <! {defaultMessage with Action = "RegisterAccount"}

Thread.Sleep(100)

// Setup random subscriptions
// for client in clients do
//   let subsciptionCount = pickRandom randomSubcribptionCount
//   for i in 1..subsciptionCount do
//     client <! {defaultMessage with Action = "Subscribe"; ToSubscribe = "twitterClient"+(string)(pickRandom [1..noOfClients])}

// Setup Zipf subscriptions
let mutable divisor = 0
for client in clients do
  divisor <- divisor + 1
  for i = noOfClients downto (noOfClients-((noOfClients-1)/divisor) + 1) do
    client <! {defaultMessage with Action = "Subscribe"; ToSubscribe = "twitterClient"+(string)(i)}

// zipf distribution tweets
let mutable tweetFrequency = noOfClients+1
for client in clients do
  tweetFrequency <- tweetFrequency - 1
  system.Scheduler.ScheduleTellRepeatedly(
    TimeSpan.FromMilliseconds(1.0 * (float)tweetFrequency),
    TimeSpan.FromMilliseconds(1.0 * (float)tweetFrequency),
    client,
    {defaultMessage with Action = "Tweet";}
  )
// Random clients Online/Offline
for client in clients do
  system.Scheduler.ScheduleTellRepeatedly(
    TimeSpan.FromSeconds(0.5),
    TimeSpan.FromSeconds(0.5),
    client,
    {defaultMessage with Action = "Online";}
  )

Console.ReadLine() |> ignore