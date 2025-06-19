# KV Log

Goral allows you to append key-value data to Google spreadsheet. Let's take an example. You provide a SAAS for wholesalers and have several services, let's say "Orders" and "Marketing Campaigns"[^usecase]. 
Your client asks you for a billing data for each of the services in a spreadsheet format at the end of the month. You turn on KV Goral service with the following configuration:

<details open>
  <summary>Basic configuration</summary>

```toml
[kv]
spreadsheet_id = "<spreadsheet_id>"
port = 49152 # port from the range 49152-65535
```
</details>

<details>
  <summary>Full configuration</summary>
  
```toml
[kv]
spreadsheet_id = "<spreadsheet_id>"
port = <"port from the range 49152-65535">
# autotruncate_at_usage_percent = 100
# messenger.url = "<messenger api url for sending messages>"
```
</details>

Such a configuration runs a server process in the Goral daemon listening at the specified port (localhost only for security reasons). From your app you periodically make a batch POST request to `localhost:<port>/api/kv` with a json body:
```json
{
  "rows": [
    {
        "datetime": "2023-12-09T09:50:46.136945094Z", /* an RFC 3339 and ISO 8601 date and time string */
        "log_name": "orders", /* validated against regex ^[a-zA-Z_][a-zA-Z0-9_]*$ */
        "data": [["donuts", 10], ["chocolate bars", 3]], /* first datarow in "orders" log defines order of column headers for a sheet (if it should be created) */
    },
    {
        "datetime": "2023-12-09T09:50:46.136945094Z",
        "log_name": "orders",
        "data": [["chocolate bars", 3], ["donuts", 0]], /* you should provide the same keys (but the order is not important) for all datarows which go to the same sheet, otherwise a separate sheet is created */
    },
    {
        "datetime": "2023-12-09T09:50:46.136945094Z",
        "log_name": "campaigns",
        "data": [["name", "10% discount for buying 3 donuts"], ["is active", true], ["credits", -6], ["datetime", "2023-12-11 09:19:32.827321506"]], /* datatypes for values are string, integer (unsigned 32-bits), boolean, float (64 bits) and datetime string (in common formats without tz) */
    }
  ]
}
```



Goral KV service responds 200 OK for a successful append.
Goral accepts every batch and creates corresponding sheets in the configured spreadsheet for "Orders" and "Marketing Campaigns". At the end of the month you have all the billing data neatly collected.
For even more interactive setup you can share a spreadsheet access with your client (for him/her see all the process online) and configure a messenger for alerts and notifications (see the section [Rules](./rules.md)) by adding your client to the chat.
Unlike other Goral services, this KV api is synchronous - if Goral responds successfully then sheets are created already and data is saved.

If there is an error while appending data, it is sent only via a default messenger of General service. Configured messenger is only used for notifications according to the configured rules.

>*Note*: KV accepts unsigned 32-bits so the integers range is `[0, 4_294_967_295]`. As Google Sheets only accepts `f64` ([doubles](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue)), then the lossless conversion from integers to floats is only valid for unsigned 32-bits. Goral internally uses lossy conversion from unsigned 64-bits integers to `f64` for those cases where rounding errors are acceptable, such as system measurements (disk space, memory etc.). If rounding errors are acceptable in your case, you should use floats for values exceeding the above mentioned range.

>*Note*: Appending to the log is *not idempotent*, i.e. if you retry the same request two times, then the same set of rows will be added twice. If it is absolutely important to avoid any duplication (which may happen in case of some unexpected failure or retrying logic), then it is recommended to some unique idempotence key to `data` to be able to filter duplicates in the spreadsheet and remove them manually.

>*Note*: for KV service the autotruncation mechanism is turned off by default (`autotruncate_at_usage_percent = 100`). It means that you should either set that value to some percent below 100 or clean up old data manually.

>*Note*: For every append operation Goral uses 2 Google api method calls, so under the quota limit of 300 requests per minute, we have 5 requests per second or 2 append operations (not considering other Goral services which use the same quota). That's why it is strongly recommended to use a batched approach (say, send rows in batches every 10 seconds or so) otherwise you can exhaust [Google api quota](https://developers.google.com/sheets/api/limits) quickly (especially when other Goral services run). [Exponential backoff algorithm](https://developers.google.com/sheets/api/limits#exponential) is *not* applicable to KV service induced requests (in contrast to other Goral services). So retries are on the client app side and you may expect http response status code `429: Too many requests` in case if you generate an excessive load. And it can impact other Goral services.
In any case Goral puts KV requests in the messages queue with a capacity 1, so any concurrent request will wait until the previous one is handled.


[^usecase]: Another notable use case is to log console errors from the frontend - catch JS exceptions, accumulate them in some batch at your backend and send the batch to the Goral KV service.
