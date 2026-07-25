mod discourse_chat;

use color_eyre::eyre::{Context as _, OptionExt, Result, eyre};
use futures::{Sink, Stream, StreamExt, sink::SinkExt};
use irc::proto::{
    CapSubCommand,
    Command::{self},
    IrcCodec, Message, Response,
    error::ProtocolError,
    message::Tag,
};
#[allow(unused_imports)]
use reqwest::Proxy;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    num::NonZeroU8,
    rc::Rc,
    str::FromStr,
};
use strum::{EnumString, IntoStaticStr, VariantNames};
use time::format_description::well_known::{Iso8601, iso8601};
use tokio::{net::TcpListener, task};
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::discourse_chat::{ChatClient, ChatMessage, MessageBus, MessageBusMessage};

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

enum Event {
    Irc(Message),
    MessageBus(MessageBusMessage),
}

#[derive(Clone, Copy, EnumString, IntoStaticStr, VariantNames, PartialEq, Eq, Hash)]
#[strum(serialize_all = "kebab-case")]
enum Capability {
    ServerTime,
    Batch,
    MessageTags,
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
    users: HashMap<i64, String>,
    connection_state: ConnectionState,
    capabilities: HashSet<Capability>,
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
            users: HashMap::new(),
            connection_state: ConnectionState::Initial,
            capabilities: HashSet::new(),
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

        self.send_backlog(&backlog).await?;

        Ok(())
    }

    async fn send_backlog(&mut self, backlog: &[ChatMessage]) -> Result<()> {
        let batch_id = if self.capabilities.contains(&Capability::Batch) {
            Some(Uuid::new_v4().to_string())
        } else {
            None
        };
        if let Some(ref batch_id) = batch_id {
            self.irc_sink
                .feed(Message::new(
                    None,
                    "BATCH",
                    vec![&format!("+{batch_id}"), "chathistory", "#blanket-fort"],
                )?)
                .await?;
        }

        for message in backlog
            .iter()
            .flat_map(|message| self.chat_message_to_irc(message))
            .collect::<Vec<_>>()
        {
            let mut message = message;
            if let Some(ref batch_id) = batch_id {
                message
                    .tags
                    .get_or_insert_with(Vec::new)
                    .push(Tag("batch".to_string(), Some(batch_id.to_owned())));
            }

            self.irc_sink.feed(message).await?;
        }

        if let Some(ref batch_id) = batch_id {
            self.irc_sink
                .feed(Message::new(None, "BATCH", vec![&format!("-{batch_id}")])?)
                .await?;
        }

        Ok(())
    }

    fn chat_message_to_irc(&self, message: &ChatMessage) -> Vec<Message> {
        let transform = |(idx, line)| {
            let mut basic_message = Message::new(
                Some(&message.sender),
                "PRIVMSG",
                vec!["#blanket-fort", line],
            )
            .unwrap();

            if self.capabilities.contains(&Capability::ServerTime)
                || self.capabilities.contains(&Capability::MessageTags)
            {
                basic_message.tags.get_or_insert_with(Vec::new).push(Tag(
                    "time".to_string(),
                    Some(
                        message
                            .timestamp
                            .format(&Iso8601::<ISO8601_CONFIG>)
                            .unwrap(),
                    ),
                ));
            }

            if self.capabilities.contains(&Capability::MessageTags) {
                basic_message.tags.get_or_insert_with(Vec::new).push(Tag(
                    "msgid".to_string(),
                    Some(if idx > 0 {
                        format!("{0}_{1}", message.id, idx)
                    } else {
                        message.id.to_string()
                    }),
                ));
            }

            basic_message
        };
        message.text.lines().enumerate().map(transform).collect()
    }

    fn users_list(&self) -> Vec<String> {
        let mut users: Vec<_> = dbg!(&self.users).values().cloned().collect();
        if !users.iter().any(|u| u.eq_ignore_ascii_case(&self.nick)) {
            users.push(self.nick.clone());
        }
        users
    }

    async fn send_names(&mut self, channel: String) -> Result<()> {
        if channel.eq_ignore_ascii_case("#blanket-fort") {
            let users = self.users_list().join(" ");
            let arguments = vec!["=".to_string(), channel.clone(), users];

            self.irc_sink
                .feed(create_response(
                    Response::RPL_NAMREPLY,
                    self.nick.clone(),
                    arguments,
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
        dbg!(&irc_message);
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
        match message.channel.as_str() {
            "/presence/chat/online" => {
                let (entering, leaving) = message.deserialize_presence();

                // TODO: Make the display of JOIN and PART messages configurable, since some clients
                // show them in quite a distracting way
                for u in entering {
                    if self.users.insert(u.id, u.username.clone()).is_none()
                        && !u.username.eq_ignore_ascii_case(&self.nick)
                        && matches!(self.connection_state, ConnectionState::Registered(_))
                    {
                        self.irc_sink
                            .feed(Message::new(
                                Some(&u.username),
                                "JOIN",
                                vec!["#blanket-fort"],
                            )?)
                            .await?;
                    }
                }

                for id in leaving {
                    if let Some(username) = self.users.remove(&id)
                        && !username.eq_ignore_ascii_case(&self.nick)
                        && matches!(self.connection_state, ConnectionState::Registered(_))
                    {
                        self.irc_sink
                            .feed(Message::new(
                                Some(&username),
                                "PART",
                                vec!["#blanket-fort"],
                            )?)
                            .await?;
                    }
                }

                self.irc_sink.flush().await?;
            }
            "/chat/4" => {
                if let ConnectionState::Registered(registered_state) = &self.connection_state {
                    let content = message.deserialize_chat_message();
                    match content {
                        Ok(content) => {
                            if !registered_state
                                .ignore_message_ids
                                .borrow()
                                .contains(&content.id)
                            {
                                for message in self.chat_message_to_irc(&content) {
                                    self.irc_sink.send(message).await?;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = dbg!(e);
                        }
                    }
                }
            }
            _ => (),
        }

        Ok(())
    }

    async fn handle(mut self) -> Result<()> {
        self.chat_client
            .list_users()
            .await?
            .into_iter()
            .for_each(|u| {
                self.users.insert(u.id, u.username);
            });

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
        let tags = irc_message.tags.unwrap_or_default();
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
            Command::Raw(command, args) if command.eq_ignore_ascii_case("TAGMSG") => {}
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
                if mask.eq_ignore_ascii_case(&self.nick) {
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
                } else if mask.eq_ignore_ascii_case("#blanket-fort") {
                    let users = self.users_list();
                    for user in users {
                        self.irc_sink
                            .feed(create_response(
                                Response::RPL_WHOREPLY,
                                self.nick.clone(),
                                vec![
                                    "#blanket-fort".to_string(),
                                    user.clone(),
                                    "blanket-fort".to_string(),
                                    "localhost".to_string(),
                                    user.clone(),
                                    "H".to_string(),
                                    format!("0 {user}"),
                                ],
                            ))
                            .await?;
                    }
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
            Command::CAP(nick, command, param, idk) => {
                self.cap_command(nick, command, param, idk).await?;
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
            Command::ChannelMODE(channel, modes) => {
                if !channel.eq_ignore_ascii_case("#blanket-fort") {
                    self.irc_sink
                        .feed(create_response(
                            Response::ERR_NOSUCHCHANNEL,
                            self.nick.clone(),
                            vec![channel, "No such channel".to_string()],
                        ))
                        .await?;
                    return Ok(());
                }

                if !modes.is_empty() {
                    self.irc_sink
                        .feed(create_response(
                            Response::ERR_CHANOPRIVSNEEDED,
                            self.nick.clone(),
                            vec![channel, "You're not channel operator".to_string()],
                        ))
                        .await?;
                    return Ok(());
                }

                self.irc_sink
                    .feed(create_response(
                        Response::RPL_CHANNELMODEIS,
                        self.nick.clone(),
                        vec![channel, "+b".to_string()],
                    ))
                    .await?;
            }
            Command::USERHOST(nicks) => {
                let users_list = self.users_list();
                // TODO: I really gotta get rid of this bad habit
                let replies = nicks
                    .into_iter()
                    .filter(|n| users_list.iter().any(|u| u.eq_ignore_ascii_case(n)))
                    .map(|n| format!("{n}=+{n}@blanket-fort"))
                    .collect::<Vec<_>>()
                    .join(" ");

                self.irc_sink
                    .feed(create_response(
                        Response::RPL_USERHOST,
                        self.nick.clone(),
                        vec![replies],
                    ))
                    .await?;
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

        fn cap_response<'a>(subcommand: &'a str, nick: &'a str, parameters: &[&'a str]) -> Message {
            let mut args = vec![nick, subcommand];
            let caps_arg = parameters.join(" ");
            args.push(&caps_arg);
            // args.extend_from_slice(parameters);

            Message::new(None, "CAP", args).unwrap()
        }

        match command {
            CapSubCommand::LS => {
                self.irc_sink
                    .feed(cap_response("LS", nick, Capability::VARIANTS))
                    .await?
            }
            CapSubCommand::LIST => {
                self.irc_sink
                    .feed(cap_response(
                        "LIST",
                        nick,
                        &self.capabilities.iter().map(Into::into).collect::<Vec<_>>(),
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

                for (cap, _) in add_extensions {
                    self.capabilities.insert(cap);
                }
                for (cap, _) in &remove_extensions {
                    self.capabilities.remove(cap);
                }

                self.irc_sink
                    .feed(cap_response("ACK", nick, &[&param]))
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
    let message_bus = MessageBus::new(chat_client, &["/chat/4", "/presence/chat/online"])
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
