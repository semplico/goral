# Setup

To use Goral you need to have a Google account and obtain a service account:
1) [Create a project](https://console.cloud.google.com/projectcreate) (we suggest creating a separate project for Goral for security reasons)
2) Enable Sheets from the [products page](https://console.cloud.google.com/workspace-api/products)
3) Create a [service account](https://console.cloud.google.com/iam-admin/serviceaccounts) with `Editor` role
4) Create a private key in JSON (three dots menu in Actions column -> Manage keys -> Add Key) for it (the private key should be downloaded by your browser)
5) Create a spreadsheet (where the scraped data will be stored) and share the spreadsheet with the service account email (you can take `client_email` from the downloaded private key) as an `Editor` 
6) Extract spreadsheet id from the spreadsheet url

Steps 5-6 can be repeated for each [service](./services.md) (it is recommended to have separate spreadsheets for usability and separation of concerns).

*Note*: you can also install Google Sheets app for your phone to have an access to the data.

Notifications are sent to messengers with three levels:
* 🟢 (INFO)
* 🟡 (WARN)
* 🔴 (ERROR)

and are prefixed with id (the argument for `--id` flag). Messengers are configured separately for every service so that you can use e.g. Discord for General service and Telegram for Healthcheck.

### Discord

<details>
  <summary>Webhook creation</summary>

1) Create a text channel
2) In the settings of the channel (cogwheel) go to the Integrations -> Webhooks
3) Either use the default one or create a new webhook
</details>

Example configuration:

```toml
messenger.url = "https://discord.com/api/webhooks/<webhook_id>/<webhook_token>"
```

[Rate limits](https://discord.com/developers/docs/topics/rate-limits).

### Slack

<details>
  <summary>App creation</summary>

Follow the [quickstart guide](https://api.slack.com/start/quickstart) and the [posting guide](https://api.slack.com/tutorials/tracks/posting-messages-with-curl).
</details>

Example configuration:

```toml
messenger.specific.token = "xoxb-slack-token"
messenger.specific.channel = "CHRISHWFH2"
messenger.url = "https://slack.com/api/chat.postMessage"
```

[Rate limit](https://api.slack.com/methods/chat.postMessage#rate_limiting)

### Telegram

<details>
  <summary>Bot creation</summary>

1) [Create a bot](https://core.telegram.org/bots/features#creating-a-new-bot)
2) Create a private group for notifications to be sent to
3) Add your bot to the group
4) Obtain a `chat_id` following the [accepted answer](https://stackoverflow.com/questions/33858927/how-to-obtain-the-chat-id-of-a-private-telegram-channel)
</details>

Example configuration:

```toml
messenger.specific.chat_id = "-100129008371"
messenger.url = "https://api.telegram.org/bot<bot token>/sendMessage"
```

[Rate limit](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)

*Note*: for Telegram all info-level messages are sent without notification so the phone doesn't vibrate or make any sound.