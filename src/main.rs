use color_eyre::eyre::{Context as _, OptionExt, Result, eyre};
use either::Either;
use futures::{
    FutureExt, Sink, Stream, StreamExt,
    future::LocalBoxFuture,
    io,
    sink::SinkExt,
    stream::{self, LocalBoxStream},
};
use irc::proto::{
    Command::{self},
    IrcCodec, Message, Response,
};
use pin_project::pin_project;
use reqwest::{Client, Proxy, header::HeaderMap};
use serde::Deserialize;
use std::pin::pin;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll, ready},
};
use time::{UtcDateTime, format_description::well_known::Iso8601};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::io::StreamReader;
use tokio_util::{
    bytes::Bytes,
    codec::{Decoder, FramedRead},
};
use uuid::Uuid;

#[derive(Deserialize, Debug)]
struct DiscourseUser {
    username: String,
}
#[derive(Deserialize, Debug)]
struct DiscourseMessage {
    message: String,
    user: DiscourseUser,
    id: i64,
}

#[allow(dead_code)]
#[derive(Deserialize, Clone)]
struct MessageBusMessage {
    global_id: i64,
    message_id: i64,
    channel: String,
    data: serde_json::Value,
}

struct MessageBusCodec {}

impl MessageBusCodec {
    const DELIMITER: &[u8] = "\r\n|\r\n".as_bytes();

    fn parse_message(
        &self,
        bytes: Bytes,
    ) -> std::result::Result<<Self as Decoder>::Item, <Self as Decoder>::Error> {
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}

impl Decoder for MessageBusCodec {
    type Item = Vec<MessageBusMessage>;
    type Error = io::Error;

    fn decode(
        &mut self,
        buf: &mut tokio_util::bytes::BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        let offset = buf.windows(5).position(|bytes| bytes == Self::DELIMITER);

        if let Some(offset) = offset {
            let mut chunk = buf.split_to(offset + Self::DELIMITER.len());
            chunk.truncate(chunk.len() - Self::DELIMITER.len());
            let chunk = chunk.freeze();
            let messages = self.parse_message(chunk);

            Ok(Some(messages?))
        } else {
            Ok(None)
        }
    }
}

type MessageBusStreamItem =
    std::result::Result<MessageBusMessage, <MessageBusCodec as Decoder>::Error>;

#[pin_project(project = MessageBusStateProj)]
enum MessageBusState<'a> {
    Connected(LocalBoxStream<'a, MessageBusStreamItem>),
    Connecting(LocalBoxFuture<'a, Result<LocalBoxStream<'a, MessageBusStreamItem>>>),
}

#[derive(Clone)]
struct MessageBusInner {
    client: Client,
    headers: HeaderMap,
    channels: HashMap<String, i64>,
    client_id: u128,
    sequence_number: i64,
}

impl MessageBusInner {
    async fn get_stream<'a>(mut self) -> Result<LocalBoxStream<'a, MessageBusStreamItem>> {
        self.channels
            .insert("__seq".to_string(), self.sequence_number);
        let response = self
            .client
            .post(format!(
                "https://a-lilian-garden.discourse.group/message-bus/{:#x}/poll",
                self.client_id
            ))
            .form(&self.channels)
            .headers(self.headers)
            .send()
            .await?;

        let messages = FramedRead::new(
            StreamReader::new(response.bytes_stream().map(|r| r.map_err(io::Error::other))),
            MessageBusCodec {},
        )
        .flat_map(|item| {
            let msgs: Vec<_> = match item {
                Ok(inner) => inner.iter().map(|x| Ok(x.clone())).collect(),
                Err(e) => vec![Err(e)],
            };

            stream::iter(msgs)
        });

        Ok(messages.boxed_local())
    }
}

#[pin_project]
struct MessageBus<'a> {
    inner: MessageBusInner,
    #[pin]
    state: MessageBusState<'a>,
}

impl MessageBus<'_> {
    fn new(client: Client, headers: HeaderMap, channels: &[&str]) -> Self {
        let client_id: u128 = rand::random();
        let channels = channels
            .iter()
            .map(|channel| (channel.to_string(), -1))
            .collect();

        let inner = MessageBusInner {
            client,
            headers,
            channels,
            client_id,
            sequence_number: 1,
        };
        let future = inner.clone().get_stream().boxed_local();

        Self {
            inner,
            state: MessageBusState::Connecting(future),
        }
    }
}

impl Stream for MessageBus<'_> {
    type Item = Result<MessageBusMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            MessageBusStateProj::Connected(stream) => {
                if let Some(message) = ready!(stream.as_mut().poll_next(cx)) {
                    // TODO: Handle __status properly
                    if let Ok(MessageBusMessage {
                        message_id,
                        channel,
                        ..
                    }) = &message
                        && let Some(entry) = self.inner.channels.get_mut(channel)
                    {
                        *entry = *message_id;
                    }
                    return Poll::Ready(Some(message.map_err(Into::into)));
                }

                this.inner.sequence_number += 1;
                let future = this.inner.clone().get_stream().boxed_local();

                this.state.set(MessageBusState::Connecting(future));

                self.poll_next(cx)
            }
            MessageBusStateProj::Connecting(future) => match ready!(future.as_mut().poll(cx)) {
                Ok(stream) => {
                    self.state = MessageBusState::Connected(stream);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
        }
    }
}

trait VariantName {
    fn variant_name(&self) -> String;
}

impl VariantName for Command {
    fn variant_name(&self) -> String {
        match self {
            Command::PASS(_) => "PASS".to_string(),
            Command::NICK(_) => "NICK".to_string(),
            Command::USER(_, _, _) => "USER".to_string(),
            Command::OPER(_, _) => "OPER".to_string(),
            Command::UserMODE(_, _) => "UserMODE".to_string(),
            Command::SERVICE(_, _, _, _, _, _) => "SERVICE".to_string(),
            Command::QUIT(_) => "QUIT".to_string(),
            Command::SQUIT(_, _) => "SQUIT".to_string(),
            Command::JOIN(_, _, _) => "JOIN".to_string(),
            Command::PART(_, _) => "PART".to_string(),
            Command::ChannelMODE(_, _) => "ChannelMODE".to_string(),
            Command::TOPIC(_, _) => "TOPIC".to_string(),
            Command::NAMES(_, _) => "NAMES".to_string(),
            Command::LIST(_, _) => "LIST".to_string(),
            Command::INVITE(_, _) => "INVITE".to_string(),
            Command::KICK(_, _, _) => "KICK".to_string(),
            Command::PRIVMSG(_, _) => "PRIVMSG".to_string(),
            Command::NOTICE(_, _) => "NOTICE".to_string(),
            Command::MOTD(_) => "MOTD".to_string(),
            Command::LUSERS(_, _) => "LUSERS".to_string(),
            Command::VERSION(_) => "VERSION".to_string(),
            Command::STATS(_, _) => "STATS".to_string(),
            Command::LINKS(_, _) => "LINKS".to_string(),
            Command::TIME(_) => "TIME".to_string(),
            Command::CONNECT(_, _, _) => "CONNECT".to_string(),
            Command::TRACE(_) => "TRACE".to_string(),
            Command::ADMIN(_) => "ADMIN".to_string(),
            Command::INFO(_) => "INFO".to_string(),
            Command::SERVLIST(_, _) => "SERVLIST".to_string(),
            Command::SQUERY(_, _) => "SQUERY".to_string(),
            Command::WHO(_, _) => "WHO".to_string(),
            Command::WHOIS(_, _) => "WHOIS".to_string(),
            Command::WHOWAS(_, _, _) => "WHOWAS".to_string(),
            Command::KILL(_, _) => "KILL".to_string(),
            Command::PING(_, _) => "PING".to_string(),
            Command::PONG(_, _) => "PONG".to_string(),
            Command::ERROR(_) => "ERROR".to_string(),
            Command::AWAY(_) => "AWAY".to_string(),
            Command::REHASH => "REHASH => todo!".to_string(),
            Command::DIE => "DIE => todo!".to_string(),
            Command::RESTART => "RESTART => todo!".to_string(),
            Command::SUMMON(_, _, _) => "SUMMON".to_string(),
            Command::USERS(_) => "USERS".to_string(),
            Command::WALLOPS(_) => "WALLOPS".to_string(),
            Command::USERHOST(_) => "USERHOST".to_string(),
            Command::ISON(_) => "ISON".to_string(),
            Command::SAJOIN(_, _) => "SAJOIN".to_string(),
            Command::SAMODE(_, _, _) => "SAMODE".to_string(),
            Command::SANICK(_, _) => "SANICK".to_string(),
            Command::SAPART(_, _) => "SAPART".to_string(),
            Command::SAQUIT(_, _) => "SAQUIT".to_string(),
            Command::NICKSERV(_) => "NICKSERV".to_string(),
            Command::CHANSERV(_) => "CHANSERV".to_string(),
            Command::OPERSERV(_) => "OPERSERV".to_string(),
            Command::BOTSERV(_) => "BOTSERV".to_string(),
            Command::HOSTSERV(_) => "HOSTSERV".to_string(),
            Command::MEMOSERV(_) => "MEMOSERV".to_string(),
            Command::CAP(_, _, _, _) => "CAP".to_string(),
            Command::AUTHENTICATE(_) => "AUTHENTICATE".to_string(),
            Command::ACCOUNT(_) => "ACCOUNT".to_string(),
            Command::METADATA(_, _, _) => "METADATA".to_string(),
            Command::MONITOR(_, _) => "MONITOR".to_string(),
            Command::BATCH(_, _, _) => "BATCH".to_string(),
            Command::CHGHOST(_, _) => "CHGHOST".to_string(),
            Command::Response(_, _) => "Response".to_string(),
            Command::Raw(_, _) => "Raw".to_string(),
        }
    }
}

struct ChatMessage {
    text: String,
    sender: String,
    id: i64,
}

impl ChatMessage {
    fn to_irc(&self) -> Vec<Result<Message>> {
        self.text
            .lines()
            .map(|line| {
                Ok(Message::new(
                    Some(&self.sender),
                    "PRIVMSG",
                    vec!["#blanket-fort", line],
                )?)
            })
            .collect()
    }
}

impl From<&DiscourseMessage> for ChatMessage {
    fn from(value: &DiscourseMessage) -> Self {
        ChatMessage {
            text: value.message.clone(),
            sender: value.user.username.clone(),
            id: value.id,
        }
    }
}

impl From<DiscourseMessage> for ChatMessage {
    fn from(value: DiscourseMessage) -> Self {
        ChatMessage {
            text: value.message,
            sender: value.user.username,
            id: value.id,
        }
    }
}

fn create_response(response: Response, client: String, arguments: Vec<String>) -> Message {
    let mut full_arguments = Vec::with_capacity(arguments.len() + 1);
    full_arguments.push(client);
    full_arguments.extend(arguments);

    Message {
        tags: None,
        prefix: None,
        command: Command::Response(response, full_arguments),
    }
}

struct ChatClient {
    client: Client,
    headers: HeaderMap,
}

impl ChatClient {
    async fn new() -> Result<Self> {
        let client = Client::builder()
            .cookie_store(true)
            .user_agent("DiscourseIRC/0.0.1")
            // .proxy(Proxy::all("http://127.0.0.1:8080")?)
            .build()?;
        let csrf_token = client
            .get("https://a-lilian-garden.discourse.group/session/csrf.json")
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?["csrf"]
            .as_str()
            .ok_or_eyre("CSRF token was not a string")?
            .to_owned();

        let mut headers = HeaderMap::new();
        headers.insert("X-CSRF-Token", csrf_token.parse()?);
        headers.insert("X-Requested-With", "XMLHttpRequest".parse()?);

        client
            .post("https://a-lilian-garden.discourse.group/session.json")
            .headers(headers.clone())
            .form(&HashMap::from([
                ("login", "angalexik"),
                ("password", "Straight-up just my literal password stored in plaintext"),
            ]))
            .send()
            .await?
            .error_for_status()?;

        Ok(Self { client, headers })
    }

    async fn message_backlog(&self) -> Result<Vec<ChatMessage>> {
        #[derive(Deserialize)]
        struct ApiResponse {
            messages: Vec<DiscourseMessage>,
        }

        let messages = self
            .client
            .get("https://a-lilian-garden.discourse.group/chat/api/channels/4/messages")
            .headers(self.headers.clone())
            .query(&[("page_size", 50)])
            .send()
            .await?
            .json::<ApiResponse>()
            .await?
            .messages;

        Ok(messages.iter().map(|m| m.into()).collect())
    }

    async fn send_message(&self, text: &str) -> Result<i64> {
        #[derive(Deserialize)]
        struct ApiResponse {
            success: String,
            message_id: i64,
        }

        let timestamp = UtcDateTime::now().format(&Iso8601::DEFAULT)?;
        let response = self
            .client
            .post("https://a-lilian-garden.discourse.group/chat/4")
            .headers(self.headers.clone())
            .form(&[
                ("message", text),
                ("staged_id", &Uuid::new_v4().to_string()),
                ("client_created_at", &timestamp),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse>()
            .await?;

        let status = response.success;
        if status == "OK" {
            Ok(response.message_id)
        } else {
            Err(eyre!("expected status `OK`, instead received `{status}`"))
        }
    }

    async fn list_users(&self) -> Result<Vec<String>> {
        #[derive(Deserialize)]
        struct GlobalPresenceChannelState {
            users: Vec<DiscourseUser>,
        }

        #[derive(Deserialize)]
        struct ApiResponse {
            global_presence_channel_state: GlobalPresenceChannelState,
        }

        Ok(self
            .client
            .get("https://a-lilian-garden.discourse.group/chat/api/me/channels")
            .headers(self.headers.clone())
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse>()
            .await?
            .global_presence_channel_state
            .users
            .into_iter()
            .map(|user| user.username)
            .collect())
    }
}

// naming is my passion
fn deserialize_messagebus_chat_message(data: serde_json::Value) -> Result<ChatMessage> {
    #[derive(Deserialize, Debug)]
    #[serde(tag = "type")]
    #[serde(rename_all = "lowercase")]
    enum Data {
        Sent { chat_message: DiscourseMessage },
    }

    let deserialized = serde_json::from_value::<Data>(data)?;

    if let Data::Sent { chat_message } = deserialized {
        Ok((&chat_message).into())
    } else {
        Err(eyre!(
            "Expected variant Data::Sent, instead got {:?}",
            deserialized
        ))
    }
}

struct Connection {
    connected: bool,
    nick: String,
}

impl Connection {
    fn new() -> Self {
        Self {
            connected: false,
            nick: "".to_string(),
        }
    }

    async fn greet_client<S>(&self, irc_sink: &mut S, chat_client: &ChatClient) -> Result<()>
    where
        S: Sink<Message> + Unpin,
        S::Error: core::error::Error + Sync + Send + 'static,
    {
        irc_sink
            .feed(Message {
                tags: None,
                prefix: None,
                command: Command::Response(
                    Response::RPL_WELCOME,
                    vec![self.nick.clone(), format!("Meow, {}", self.nick)],
                ),
            })
            .await?;

        irc_sink
            .feed(Message {
                tags: None,
                prefix: None,
                command: Command::Response(
                    Response::RPL_YOURHOST,
                    vec![
                        self.nick.clone(),
                        "Discourse Chat IRC Proxy version 0.0.1".to_string(),
                    ],
                ),
            })
            .await?;

        irc_sink
            .feed(Message::new(
                Some(&self.nick),
                "JOIN",
                vec!["#blanket-fort"],
            )?)
            .await?;

        self.send_names(irc_sink, &chat_client, "#blanket-fort".to_string())
            .await?;

        let backlog = chat_client.message_backlog().await?;

        for message in backlog.iter().flat_map(|message| message.to_irc()) {
            irc_sink.feed(message?).await?;
        }

        Ok(())
    }

    async fn send_names<S>(
        &self,
        irc_sink: &mut S,
        chat_client: &ChatClient,
        channel: String,
    ) -> Result<()>
    where
        S: Sink<Message> + Unpin,
        S::Error: core::error::Error + Sync + Send + 'static,
    {
        if channel == "#blanket-fort" {
            let users = chat_client.list_users().await?;
            let arguments = vec!["=".to_string(), channel.clone()];

            irc_sink
                .feed(create_response(
                    Response::RPL_NAMREPLY,
                    self.nick.clone(),
                    [arguments, users].concat(),
                ))
                .await?;

            irc_sink
                .feed(create_response(
                    Response::RPL_ENDOFNAMES,
                    self.nick.clone(),
                    vec![channel, "End of /NAMES list".to_string()],
                ))
                .await?;
        } else {
            irc_sink
                .feed(create_response(
                    Response::ERR_NOSUCHCHANNEL,
                    self.nick.clone(),
                    vec![channel, "No such channel".to_string()],
                ))
                .await?;
        }

        Ok(())
    }

    async fn handle(&mut self, socket: TcpStream) -> Result<()> {
        let (mut irc_sink, irc_stream) = IrcCodec::new("UTF-8")?.framed(socket).split();
        let irc_stream = irc_stream.map(Either::Right);
        let chat_client = ChatClient::new().await?;

        // TODO: Replace with HashSet and remove old values to prevent memory leak
        let mut ignore_message_ids = Vec::new();
        let message_bus = MessageBus::new(
            chat_client.client.clone(),
            chat_client.headers.clone(),
            &["/chat/4"],
        )
        .map(Either::Left);

        let mut ultimate_stream = tokio_stream::StreamExt::merge(irc_stream, message_bus);
        while let Some(item) = ultimate_stream.next().await {
            match item {
                Either::Right(irc_message) => {
                    match dbg!(irc_message?).command {
                        Command::PING(x, y) => {
                            irc_sink
                                .feed(Message {
                                    tags: None,
                                    prefix: None,
                                    command: Command::PONG(x, y),
                                })
                                .await?
                        }
                        Command::NICK(_) => {
                            self.nick = "angalexik".to_string();

                            if !self.connected {
                                self.connected = true;

                                self.greet_client(&mut irc_sink, &chat_client).await?;
                                irc_sink.flush().await?;
                            }
                        }
                        Command::PRIVMSG(target, text) => {
                            if target == "#blanket-fort" {
                                let message_id = chat_client.send_message(&text).await?;
                                ignore_message_ids.push(dbg!(message_id));
                            } else {
                                irc_sink
                                    .feed(Message {
                                        tags: None,
                                        prefix: None,
                                        command: Command::Response(
                                            Response::ERR_NOSUCHNICK,
                                            vec![
                                                self.nick.clone(),
                                                target,
                                                "No such nick/channel".to_string(),
                                            ],
                                        ),
                                    })
                                    .await?;
                            }
                        }
                        Command::JOIN(channel, _, _) => {
                            if channel == "#blanket-fort" {
                                irc_sink
                                    .feed(Message::new(
                                        Some(&self.nick),
                                        "JOIN",
                                        vec!["#blanket-fort"],
                                    )?)
                                    .await?;
                            } else {
                                irc_sink
                                    .feed(Message {
                                        tags: None,
                                        prefix: None,
                                        command: Command::Response(
                                            Response::ERR_NOSUCHCHANNEL,
                                            vec![
                                                self.nick.clone(),
                                                channel,
                                                "No such channel".to_string(),
                                            ],
                                        ),
                                    })
                                    .await?;
                            }
                        }
                        Command::WHO(None, _) => {
                            irc_sink
                                .feed(create_response(
                                    Response::ERR_NEEDMOREPARAMS,
                                    self.nick.clone(),
                                    vec!["WHO".to_string(), "Not enough parameters".to_string()],
                                ))
                                .await?;
                        }
                        Command::WHO(Some(mask), _) => {
                            if mask.to_ascii_lowercase() == self.nick.to_ascii_lowercase()
                                || mask == "#blanket-fort"
                            {
                                irc_sink
                                    .feed(create_response(
                                        Response::RPL_WHOREPLY,
                                        self.nick.clone(),
                                        vec![
                                            "#blanket-fort".to_string(),
                                            self.nick.clone(),
                                            self.nick.clone(),
                                            "localhost".to_string(),
                                            self.nick.clone(),
                                            "H".to_string(),
                                            0.to_string(),
                                            self.nick.clone(),
                                        ],
                                    ))
                                    .await?;
                                irc_sink
                                    .feed(create_response(
                                        Response::RPL_ENDOFWHO,
                                        self.nick.clone(),
                                        vec![mask, "End of WHO list".to_string()],
                                    ))
                                    .await?;
                            } else {
                                irc_sink
                                    .feed(create_response(
                                        Response::ERR_NOSUCHNICK,
                                        self.nick.clone(),
                                        vec![mask, "No such nick/channel".to_string()],
                                    ))
                                    .await?;
                            }
                        }
                        Command::NAMES(None, _) => {
                            irc_sink
                                .feed(create_response(
                                    Response::ERR_NEEDMOREPARAMS,
                                    self.nick.clone(),
                                    vec!["NAMES".to_string(), "Not enough parameters".to_string()],
                                ))
                                .await?;
                        }
                        Command::NAMES(Some(channel), _) => {
                            self.send_names(&mut irc_sink, &chat_client, channel)
                                .await?;
                        }
                        Command::QUIT(_) => break,
                        Command::USER(_, _, _) => (),
                        other => {
                            irc_sink
                                .feed(create_response(
                                    Response::ERR_UNKNOWNCOMMAND,
                                    self.nick.clone(),
                                    vec![other.variant_name(), "Unknown command".to_string()],
                                ))
                                .await?;
                            eprintln!("Unknown method: {other:#?}")
                        }
                    }

                    irc_sink.flush().await?;
                }
                Either::Left(message_bus_message) => {
                    let message = message_bus_message?;
                    if message.channel == "/chat/4" {
                        let content = deserialize_messagebus_chat_message(message.data);
                        match content {
                            Ok(content) => {
                                if !ignore_message_ids.contains(&content.id) {
                                    for message in content.to_irc() {
                                        irc_sink.send(message?).await?;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = dbg!(e);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    println!("Hello, world!");
    color_eyre::install()?;

    let listener = TcpListener::bind("0.0.0.0:6667").await?;

    loop {
        let (socket, address) = listener.accept().await?;
        println!("Received connection from {address}");
        let mut conn = Connection::new();
        if let Err(e) = conn
            .handle(socket)
            .await
            .wrap_err_with(|| format!("Error handling connection from {address:?}"))
        {
            eprintln!("{e:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChatClient, MessageBus};

    use futures::StreamExt;

    #[tokio::test]
    async fn test_discourse_login() {
        let chat_client = ChatClient::new().await.unwrap();

        // Can only read private messages if successfully logged in
        assert!(
            chat_client
                .client
                .get(
                    "https://a-lilian-garden.discourse.group/topics/private-messages/angalexik.json"
                )
                .headers(chat_client.headers)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }

    #[tokio::test]
    async fn test_message_bus() {
        let chat_client = ChatClient::new().await.unwrap();
        let mut message_bus = MessageBus::new(
            chat_client.client,
            chat_client.headers,
            &["/refresh_client"],
        );

        assert_eq!(
            message_bus.next().await.unwrap().unwrap().channel,
            "/__status".to_string()
        )
    }
}
