use color_eyre::eyre::{Context as _, OptionExt, Result, eyre};
use futures::{
    FutureExt, Sink, Stream, StreamExt,
    future::LocalBoxFuture,
    io,
    sink::SinkExt,
    stream::{self, LocalBoxStream},
};
use irc::proto::{
    CapSubCommand,
    Command::{self},
    IrcCodec, Message, Response,
    error::ProtocolError,
    message::Tag,
};
use pin_project::pin_project;
#[allow(unused_imports)]
use reqwest::Proxy;
use reqwest::{Client, IntoUrl, Url, header::HeaderMap};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, num::NonZeroU8, pin::pin, rc::Rc, str::FromStr};
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll, ready},
};
use strum::{EnumString, IntoStaticStr};
use time::{
    UtcDateTime,
    format_description::well_known::{Iso8601, Rfc3339, iso8601},
};
use tokio::{net::TcpListener, task};
use tokio_util::io::StreamReader;
use tokio_util::{
    bytes::Bytes,
    codec::{Decoder, FramedRead},
};
use uuid::Uuid;

const ISO8601_CONFIG: iso8601::EncodedConfig = iso8601::Config::DEFAULT
    .set_time_precision(iso8601::TimePrecision::Second {
        decimal_digits: NonZeroU8::new(3),
    })
    .encode();

#[derive(Serialize, Deserialize)]
struct ServerConfig {
    base_url: String,
    username: String,
    password: String,
    channel_number: i64,
    channel_name: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            base_url: "https://example.com".to_string(),
            username: "miku_hatsune".to_string(),
            password: "hunter2".to_string(),
            channel_number: 4,
            channel_name: "#blanket-fort".to_string(),
        }
    }
}

#[derive(Deserialize, Debug)]
struct DiscourseUser {
    username: String,
}
#[derive(Deserialize, Debug)]
struct DiscourseMessage {
    message: String,
    user: DiscourseUser,
    id: i64,
    created_at: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize, Clone)]
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
    base_url: Url,
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
            .post(
                self.base_url
                    .join("/message-bus/")?
                    .join(&format!("{:#x}/", self.client_id))?
                    .join("poll")?,
            )
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
    fn new(client: Client, headers: HeaderMap, base_url: Url, channels: &[&str]) -> Self {
        let client_id: u128 = rand::random();
        let channels = channels
            .iter()
            .map(|channel| (channel.to_string(), -1))
            .collect();

        let inner = MessageBusInner {
            client,
            headers,
            base_url,
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
            Command::REHASH => "REHASH".to_string(),
            Command::DIE => "DIE".to_string(),
            Command::RESTART => "RESTART".to_string(),
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
            Command::Raw(command, _) => command.to_owned(),
        }
    }
}

struct ChatMessage {
    text: String,
    sender: String,
    timestamp: UtcDateTime,
    id: i64,
}

impl From<&DiscourseMessage> for ChatMessage {
    fn from(value: &DiscourseMessage) -> Self {
        ChatMessage {
            text: value.message.clone(),
            sender: value.user.username.clone(),
            id: value.id,
            timestamp: UtcDateTime::parse(&value.created_at, &Rfc3339).unwrap(),
        }
    }
}

impl From<DiscourseMessage> for ChatMessage {
    fn from(value: DiscourseMessage) -> Self {
        From::from(&value)
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

#[derive(Clone)]
struct ChatClient {
    client: Client,
    headers: HeaderMap,
    base_url: Url,
    username: String,
}

impl ChatClient {
    async fn new(base_url: impl IntoUrl, login: String, password: &str) -> Result<Self> {
        let base_url = base_url.into_url()?;

        let client = Client::builder()
            .cookie_store(true)
            .user_agent("DiscourseIRC/0.0.1")
            // .proxy(Proxy::all("http://127.0.0.1:8080")?)
            .build()?;
        let csrf_token = client
            .get(base_url.join("/session/csrf.json")?)
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
            .post(base_url.join("/session.json")?)
            .headers(headers.clone())
            .form(&HashMap::from([
                ("login", login.as_str()),
                ("password", password),
            ]))
            .send()
            .await?
            .error_for_status()?;

        Ok(Self {
            client,
            headers,
            base_url,
            username: login,
        })
    }

    async fn message_backlog(&self) -> Result<Vec<ChatMessage>> {
        #[derive(Deserialize)]
        struct ApiResponse {
            messages: Vec<DiscourseMessage>,
        }

        let messages = self
            .client
            .get(self.base_url.join("/chat/api/channels/4/messages")?)
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

        let timestamp = UtcDateTime::now().format(&Iso8601::<ISO8601_CONFIG>)?;
        let response = self
            .client
            .post(self.base_url.join("/chat/4")?)
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
            .get(self.base_url.join("/chat/api/me/channels")?)
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

enum Event {
    Irc(Message),
    MessageBus(MessageBusMessage),
}

#[derive(Clone, Copy, EnumString, IntoStaticStr, PartialEq, Eq)]
#[strum(serialize_all = "kebab-case")]
enum Capability {
    ServerTime,
}

#[derive(Default, Clone)]
struct RegisteredState {
    // TODO: Replace with HashSet and remove old values to prevent memory leak
    ignore_message_ids: Rc<RefCell<Vec<i64>>>,
}

#[derive(Clone)]
enum ConnectionState {
    Initial,
    Negotiating,
    Registered(RegisteredState),
}

impl ConnectionState {
    fn is_registered(&self) -> bool {
        matches!(self, ConnectionState::Registered(_))
    }
}

struct Connection<Si, St> {
    nick: String,
    irc_sink: Si,
    event_stream: St,
    chat_client: ChatClient,
    connection_state: ConnectionState,
    capabilities: Vec<Capability>,
}

impl<Si, St> Connection<Si, St>
where
    Si: Sink<Message> + Unpin,
    Si::Error: core::error::Error + Sync + Send + 'static,
    St: Stream<Item = Result<Event>> + Unpin,
{
    async fn new(irc_sink: Si, event_stream: St, chat_client: ChatClient) -> Self {
        Self {
            nick: chat_client.username.clone(),
            irc_sink,
            event_stream,
            chat_client,
            connection_state: ConnectionState::Initial,
            capabilities: Vec::new(),
        }
    }

    async fn greet_client(&mut self) -> Result<()> {
        self.irc_sink
            .feed(Message {
                tags: None,
                prefix: None,
                command: Command::Response(
                    Response::RPL_WELCOME,
                    vec![self.nick.clone(), format!("Meow, {}", self.nick)],
                ),
            })
            .await?;

        self.irc_sink
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

        self.irc_sink
            .feed(Message::new(
                Some(&self.nick),
                "JOIN",
                vec!["#blanket-fort"],
            )?)
            .await?;

        self.send_names("#blanket-fort".to_string()).await?;

        let backlog = self.chat_client.message_backlog().await?;

        for message in backlog
            .iter()
            .flat_map(|message| self.chat_message_to_irc(message))
            .collect::<Vec<_>>()
        {
            self.irc_sink.feed(message?).await?;
        }

        Ok(())
    }

    fn chat_message_to_irc(&self, message: &ChatMessage) -> Vec<Result<Message>> {
        message
            .text
            .lines()
            .map(|line| {
                if self.capabilities.contains(&Capability::ServerTime) {
                    Ok(Message::with_tags(
                        Some(vec![Tag(
                            "time".to_string(),
                            Some(
                                message
                                    .timestamp
                                    .format(&Iso8601::<ISO8601_CONFIG>)
                                    .unwrap(),
                            ),
                        )]),
                        Some(&message.sender),
                        "PRIVMSG",
                        vec!["#blanket-fort", line],
                    )?)
                } else {
                    Ok(Message::new(
                        Some(&message.sender),
                        "PRIVMSG",
                        vec!["#blanket-fort", line],
                    )?)
                }
            })
            .collect()
    }

    async fn send_names(&mut self, channel: String) -> Result<()> {
        if channel.eq_ignore_ascii_case("#blanket-fort") {
            let mut users = self.chat_client.list_users().await?;
            if !users.iter().any(|u| u.eq_ignore_ascii_case(&self.nick)) {
                users.push(self.nick.clone());
            }
            let arguments = vec!["=".to_string(), channel.clone()];

            self.irc_sink
                .feed(create_response(
                    Response::RPL_NAMREPLY,
                    self.nick.clone(),
                    [arguments, users].concat(),
                ))
                .await?;

            self.irc_sink
                .feed(create_response(
                    Response::RPL_ENDOFNAMES,
                    self.nick.clone(),
                    vec![channel, "End of /NAMES list".to_string()],
                ))
                .await?;
        } else {
            self.irc_sink
                .feed(create_response(
                    Response::ERR_NOSUCHCHANNEL,
                    self.nick.clone(),
                    vec![channel, "No such channel".to_string()],
                ))
                .await?;
        }

        Ok(())
    }

    async fn handle_irc(&mut self, irc_message: Message) -> Result<bool> {
        match irc_message.command {
            Command::PING(x, y) => {
                self.irc_sink
                    .send(Message {
                        tags: None,
                        prefix: None,
                        command: Command::PONG(x, y),
                    })
                    .await?;
                return Ok(false);
            }
            Command::QUIT(_) => return Ok(true),
            _ => (),
        }

        match self.connection_state.clone() {
            ConnectionState::Initial => self.handle_initial(irc_message).await?,
            ConnectionState::Negotiating => self.handle_negotiating(irc_message).await?,
            ConnectionState::Registered(mut registered_state) => {
                self.handle_registered(irc_message, &mut registered_state)
                    .await?
            }
        }

        self.irc_sink.flush().await?;

        Ok(false)
    }

    async fn handle_messagebus(&mut self, message: MessageBusMessage) -> Result<()> {
        if let ConnectionState::Registered(registered_state) = &self.connection_state {
            if message.channel == "/chat/4" {
                let content = deserialize_messagebus_chat_message(message.data);
                match content {
                    Ok(content) => {
                        if !registered_state
                            .ignore_message_ids
                            .borrow()
                            .contains(&content.id)
                        {
                            for message in self.chat_message_to_irc(&content) {
                                self.irc_sink.send(message?).await?;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = dbg!(e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle(mut self) -> Result<()> {
        while let Some(item) = self.event_stream.next().await {
            match item? {
                Event::Irc(irc_message) => {
                    if self.handle_irc(irc_message).await? {
                        break;
                    }
                }
                Event::MessageBus(message) => {
                    self.handle_messagebus(message).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_initial(&mut self, irc_message: Message) -> Result<()> {
        match irc_message.command {
            Command::NICK(_) => {
                self.greet_client().await?;
                self.connection_state = ConnectionState::Registered(RegisteredState::default());
            }
            Command::USER(..) => (),
            Command::PASS(_) => (),
            Command::CAP(nick, command, param, idk) => {
                self.connection_state = ConnectionState::Negotiating;
                self.cap_command(nick, command, param, idk).await?;
            }

            other => {
                self.irc_sink
                    .feed(create_response(
                        Response::ERR_UNKNOWNCOMMAND,
                        self.nick.clone(),
                        vec![other.variant_name(), "Unknown command".to_string()],
                    ))
                    .await?;
                eprintln!("Unknown method: {other:#?}");
            }
        }

        Ok(())
    }

    async fn handle_negotiating(&mut self, irc_message: Message) -> Result<()> {
        match irc_message.command {
            Command::NICK(..) => (),
            Command::USER(..) => (),
            Command::PASS(_) => (),
            Command::CAP(None, CapSubCommand::END, None, None) => {
                self.greet_client().await?;
                self.connection_state = ConnectionState::Registered(RegisteredState::default());
            }
            Command::CAP(nick, command, param, idk) => {
                self.cap_command(nick, command, param, idk).await?;
            }

            other => {
                self.irc_sink
                    .feed(create_response(
                        Response::ERR_UNKNOWNCOMMAND,
                        self.nick.clone(),
                        vec![other.variant_name(), "Unknown command".to_string()],
                    ))
                    .await?;
                eprintln!("Unknown method: {other:#?}");
            }
        }

        Ok(())
    }

    async fn handle_registered(
        &mut self,
        irc_message: Message,
        registered_state: &mut RegisteredState,
    ) -> Result<()> {
        match irc_message.command {
            Command::PRIVMSG(target, text) => {
                if target == "#blanket-fort" {
                    let message_id = self.chat_client.send_message(&text).await?;
                    registered_state
                        .ignore_message_ids
                        .borrow_mut()
                        .push(dbg!(message_id));
                } else {
                    self.irc_sink
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
                    self.irc_sink
                        .feed(Message::new(
                            Some(&self.nick),
                            "JOIN",
                            vec!["#blanket-fort"],
                        )?)
                        .await?;
                } else {
                    self.irc_sink
                        .feed(Message {
                            tags: None,
                            prefix: None,
                            command: Command::Response(
                                Response::ERR_NOSUCHCHANNEL,
                                vec![self.nick.clone(), channel, "No such channel".to_string()],
                            ),
                        })
                        .await?;
                }
            }
            Command::WHO(None, _) => {
                self.irc_sink
                    .feed(create_response(
                        Response::ERR_NEEDMOREPARAMS,
                        self.nick.clone(),
                        vec!["WHO".to_string(), "Not enough parameters".to_string()],
                    ))
                    .await?;
            }
            Command::WHO(Some(mask), _) => {
                if mask.eq_ignore_ascii_case(&self.nick)
                    || mask.eq_ignore_ascii_case("#blanket-fort")
                {
                    self.irc_sink
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
                    self.irc_sink
                        .feed(create_response(
                            Response::RPL_ENDOFWHO,
                            self.nick.clone(),
                            vec![mask, "End of WHO list".to_string()],
                        ))
                        .await?;
                } else {
                    self.irc_sink
                        .feed(create_response(
                            Response::ERR_NOSUCHNICK,
                            self.nick.clone(),
                            vec![mask, "No such nick/channel".to_string()],
                        ))
                        .await?;
                }
            }
            Command::NAMES(None, _) => {
                self.irc_sink
                    .feed(create_response(
                        Response::ERR_NEEDMOREPARAMS,
                        self.nick.clone(),
                        vec!["NAMES".to_string(), "Not enough parameters".to_string()],
                    ))
                    .await?;
            }
            Command::NAMES(Some(channel), _) => {
                self.send_names(channel).await?;
            }
            Command::NICK(new_nick) => {
                self.irc_sink
                    .feed(create_response(
                        Response::ERR_ERRONEOUSNICKNAME,
                        self.nick.clone(),
                        vec![new_nick, "Erroneus nickname".to_string()],
                    ))
                    .await?;
            }
            Command::USER(..) => {
                // self.irc_sink
                //     .feed(create_response(
                //         Response::ERR_ALREADYREGISTRED,
                //         self.nick.clone(),
                //         vec!["You may not reregister".to_string()],
                //     ))
                //     .await?;
            }
            Command::PING(..) => unreachable!(),
            Command::QUIT(..) => unreachable!(),
            other => {
                self.irc_sink
                    .feed(create_response(
                        Response::ERR_UNKNOWNCOMMAND,
                        self.nick.clone(),
                        vec![other.variant_name(), "Unknown command".to_string()],
                    ))
                    .await?;
                eprintln!("Unknown method: {other:#?}")
            }
        }

        Ok(())
    }

    async fn cap_command(
        &mut self,
        nick: Option<String>,
        command: CapSubCommand,
        param: Option<String>,
        idk: Option<String>,
    ) -> Result<()> {
        if nick.is_some() && idk.is_some() {
            return Err(eyre!("Malformed CAP command"));
        }

        let nick = if self.connection_state.is_registered() {
            &self.nick
        } else {
            "*"
        };

        fn cap_response<'a>(
            subcommand: &'a str,
            nick: &'a str,
            mut parameters: Vec<&'a str>,
        ) -> Message {
            let mut args = vec![nick, subcommand];
            args.append(&mut parameters);

            Message::new(None, "CAP", args).unwrap()
        }

        match command {
            CapSubCommand::LS => {
                self.irc_sink
                    .feed(cap_response("LS", nick, vec!["server-time"]))
                    .await?
            }
            CapSubCommand::LIST => {
                self.irc_sink
                    .feed(cap_response(
                        "LIST",
                        nick,
                        self.capabilities.iter().map(Into::into).collect(),
                    ))
                    .await?;
            }
            CapSubCommand::REQ => {
                let param = param.ok_or_eyre("no extensions listed")?;
                let (add_extensions, remove_extensions): (Vec<_>, Vec<_>) = param
                    .split(' ')
                    .filter_map(|mut p| {
                        let add;
                        if p.as_bytes()[0] == b'-' {
                            add = false;
                            let (_, rest) = p.split_at(1);
                            p = rest;
                        } else {
                            add = true;
                        }

                        Capability::from_str(p).map(|c| (c, add)).ok()
                    })
                    .partition(|&(_, add)| add);

                let mut add_extensions: Vec<_> =
                    add_extensions.into_iter().map(|(c, _)| c).collect();

                let remove_extensions: Vec<_> =
                    remove_extensions.into_iter().map(|(c, _)| c).collect();

                self.capabilities.append(&mut add_extensions);

                // O(n^2) moment
                self.capabilities = self
                    .capabilities
                    .iter()
                    .copied()
                    .filter(|c| !remove_extensions.contains(c))
                    .collect();

                self.irc_sink
                    .feed(cap_response("ACK", nick, vec![&param]))
                    .await?;
            }
            CapSubCommand::ACK | CapSubCommand::NAK | CapSubCommand::NEW | CapSubCommand::DEL => {
                return Err(eyre!("Hey! That's my line!"));
            }
            CapSubCommand::END => unreachable!(),
        }

        Ok(())
    }
}

fn create_event_stream(
    chat_client: ChatClient,
    irc_stream: impl Stream<Item = Result<Message, ProtocolError>>,
) -> impl Stream<Item = Result<Event>> {
    let irc_stream = irc_stream.map(|m| m.map(Event::Irc).map_err(Into::into));
    let message_bus = MessageBus::new(
        chat_client.client,
        chat_client.headers,
        chat_client.base_url,
        &["/chat/4"],
    )
    .map(|m| m.map(Event::MessageBus));

    tokio_stream::StreamExt::merge(irc_stream, message_bus)
}

#[tokio::main(flavor = "local")]
async fn main() -> Result<()> {
    println!("Hello, world!");
    color_eyre::install()?;

    let config: ServerConfig = confy::load("ghffdsa", Some("config"))?;

    let listener = TcpListener::bind("0.0.0.0:6667").await?;

    loop {
        let (socket, address) = listener.accept().await?;
        println!("Received connection from {address}");
        let chat_client =
            ChatClient::new(&config.base_url, config.username.clone(), &config.password).await?;
        let (irc_sink, irc_stream) = IrcCodec::new("UTF-8")?.framed(socket).split();
        let event_stream = create_event_stream(chat_client.clone(), irc_stream);

        task::spawn_local(async move {
            let conn = Connection::new(irc_sink, event_stream, chat_client).await;
            if let Err(e) = conn
                .handle()
                .await
                .wrap_err_with(|| format!("Error handling connection from {address:?}"))
            {
                eprintln!("{e:?}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::MessageBusMessage;

    use super::{ChatClient, MessageBus};

    use futures::StreamExt;
    use httpmock::{Mock, prelude::*};
    use reqwest::Url;
    use serde_json::json;

    struct MockLogin<'a> {
        mock_csrf: Mock<'a>,
        mock_session: Mock<'a>,
    }

    impl MockLogin<'_> {
        const CSRF_TOKEN: &'static str = "dummy_token";
        const SESSION_COOKIE: &'static str = "dummy_session";

        fn mock<'a>(server: &'a MockServer) -> MockLogin<'a> {
            let mock_csrf = server.mock(|when, then| {
                when.path("/session/csrf.json");
                then.status(200)
                    .json_body(json!({ "csrf": Self::CSRF_TOKEN }));
            });
            let mock_session = server.mock(|when, then| {
                when.method(POST)
                    .path("/session.json")
                    .header("X-CSRF-Token", Self::CSRF_TOKEN)
                    .form_urlencoded_tuple("login", "test_user")
                    .form_urlencoded_tuple("password", "test_password");
                then.status(200).header(
                    "Set-Cookie",
                    format!("_forum_session={}", Self::SESSION_COOKIE),
                );
            });

            MockLogin {
                mock_csrf,
                mock_session,
            }
        }
    }

    impl Drop for MockLogin<'_> {
        fn drop(&mut self) {
            self.mock_session.assert();
            self.mock_csrf.assert();
        }
    }

    #[tokio::test]
    async fn test_discourse_login() {
        let server = MockServer::start();
        // Assigned to `_mock` instead of just `_` in order for the value to get dropped at
        // the end of this function scope
        let _mock = MockLogin::mock(&server);

        let url: Url = server.base_url().as_str().try_into().unwrap();

        ChatClient::new(url.clone(), "test_user".to_string(), "test_password")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_message_bus() {
        let server = MockServer::start();
        let _mock = MockLogin::mock(&server);
        let message_bus_mock = server.mock(|when, then| {
            when.method(POST)
                .path_matches(r"/message-bus/[^/]+/poll")
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple_exists("__seq")
                .form_urlencoded_tuple("/refresh_client", "-1");
            then.status(200).body(format!(
                "{}\r\n|\r\n",
                serde_json::to_string(&vec![MessageBusMessage {
                    global_id: -1,
                    message_id: -1,
                    channel: "/__status".to_string(),
                    data: json!({}),
                }])
                .unwrap()
            ));
        });

        let url: Url = server.base_url().as_str().try_into().unwrap();
        let chat_client = ChatClient::new(url, "test_user".to_string(), "test_password")
            .await
            .unwrap();
        let mut message_bus = MessageBus::new(
            chat_client.client,
            chat_client.headers,
            chat_client.base_url,
            &["/refresh_client"],
        );

        assert_eq!(
            message_bus.next().await.unwrap().unwrap().channel,
            "/__status".to_string()
        );
        assert!(message_bus_mock.calls() > 0);
    }

    #[tokio::test]
    async fn test_new_message() {
        let server = MockServer::start();
        let _mock = MockLogin::mock(&server);

        let message_id = 2;

        let message_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/chat/4")
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .header("X-Requested-With", "XMLHttpRequest")
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple("message", "test-message")
                .form_urlencoded_tuple_exists("staged_id")
                .form_urlencoded_tuple_exists("client_created_at");
            then.status(200)
                .json_body(json!({ "success": "OK", "message_id": 2 }));
        });

        let url: Url = server.base_url().as_str().try_into().unwrap();
        let chat_client = ChatClient::new(url, "test_user".to_string(), "test_password")
            .await
            .unwrap();

        assert_eq!(
            chat_client.send_message("test-message").await.unwrap(),
            message_id
        );
        message_mock.assert();
    }
}
