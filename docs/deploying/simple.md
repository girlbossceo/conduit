# Simple setup

This is the recommended way to set up Conduit. It is the easiest way to get started and is suitable for most use cases.

### Please note that this documentation is not fully representative of conduwuit at the moment. Assume majority of it is outdated.

> ## Getting help
>
> If you run into any problems while setting up conduwuit, ask us
> in `#conduwuit:puppygock.gay` or [open an issue on GitHub](https://github.com/girlbossceo/conduwuit/issues/new).

## Installing conduwuit

You may simply download the binary that fits your machine. Run `uname -m` to see what you need.

Prebuilt binaries can be downloaded from the latest successful CI workflow on the main branch here: https://github.com/girlbossceo/conduwuit/actions/workflows/ci.yml?query=branch%3Amain+actor%3Agirlbossceo

```bash
$ sudo wget -O /usr/local/bin/matrix-conduit <url>
$ sudo chmod +x /usr/local/bin/matrix-conduit
```

Alternatively, you may compile the binary yourself. First, install any dependencies:

```bash
# Debian
$ sudo apt install libclang-dev build-essential

# RHEL
$ sudo dnf install clang
```
Then, `cd` into the source tree of conduit-next and run:
```bash
$ cargo build --release
```

If you want to cross compile Conduit to another architecture, read the guide below.

<details>
<summary>Cross compilation</summary>

As easiest way to compile conduit for another platform [cross-rs](https://github.com/cross-rs/cross) is recommended, so install it first.

In order to use RockDB as storage backend append `-latomic` to linker flags.

For example, to build a binary for Raspberry Pi Zero W (ARMv6) you need `arm-unknown-linux-gnueabihf` as compilation
target.

```bash
git clone https://gitlab.com/famedly/conduit.git
cd conduit
export RUSTFLAGS='-C link-arg=-lgcc -Clink-arg=-latomic -Clink-arg=-static-libgcc'
cross build --release --no-default-features --features conduit_bin,backend_rocksdb --target=arm-unknown-linux-gnueabihf
```
</details>

## Adding a Conduit user

While Conduit can run as any user it is usually better to use dedicated users for different services. This also allows
you to make sure that the file permissions are correctly set up.

In Debian or RHEL, you can use this command to create a Conduit user:

```bash
sudo adduser --system conduit --group --disabled-login --no-create-home
```

## Forwarding ports in the firewall or the router

Conduit uses the ports 443 and 8448 both of which need to be open in the firewall.

If Conduit runs behind a router or in a container and has a different public IP address than the host system these public ports need to be forwarded directly or indirectly to the port mentioned in the config.

## Optional: Avoid port 8448

If Conduit runs behind Cloudflare reverse proxy, which doesn't support port 8448 on free plans, [delegation](https://matrix-org.github.io/synapse/latest/delegate.html) can be set up to have federation traffic routed to port 443:
```apache
# .well-known delegation on Apache
<Files "/.well-known/matrix/server">
    ErrorDocument 200 '{"m.server": "your.server.name:443"}'
    Header always set Content-Type application/json
    Header always set Access-Control-Allow-Origin *
</Files>
```
[SRV DNS record](https://spec.matrix.org/latest/server-server-api/#resolving-server-names) delegation is also [possible](https://www.cloudflare.com/en-gb/learning/dns/dns-records/dns-srv-record/).

## Setting up a systemd service

Now we'll set up a systemd service for Conduit, so it's easy to start/stop Conduit and set it to autostart when your
server reboots. Simply paste the default systemd service you can find below into
`/etc/systemd/system/conduit.service`.

```systemd
[Unit]
Description=Conduit Matrix Server
After=network.target

[Service]
Environment="CONDUIT_CONFIG=/etc/matrix-conduit/conduit.toml"
User=conduit
Group=conduit
RuntimeDirectory=conduit
RuntimeDirectoryMode=0750
Restart=always
ExecStart=/usr/local/bin/matrix-conduit

[Install]
WantedBy=multi-user.target
```

Finally, run

```bash
$ sudo systemctl daemon-reload
```

## Creating the Conduit configuration file

Now we need to create the Conduit's config file in `/etc/conduwuit/conduwuit.toml`. Paste this in **and take a moment
to read it. You need to change at least the server name.**  
You can also choose to use a different database backend, but right now only `rocksdb` and `sqlite` are recommended.

See the following example config at [conduwuit-example.toml](../configuration.md)

## Setting the correct file permissions

As we are using a Conduit specific user we need to allow it to read the config. To do that you can run this command on
Debian or RHEL:

```bash
sudo chown -R root:root /etc/matrix-conduit
sudo chmod 755 /etc/matrix-conduit
```

If you use the default database path you also need to run this:

```bash
sudo mkdir -p /var/lib/matrix-conduit/
sudo chown -R conduit:conduit /var/lib/matrix-conduit/
sudo chmod 700 /var/lib/matrix-conduit/
```

## Setting up the Reverse Proxy

This depends on whether you use Apache, Caddy, Nginx or another web server.

### Apache

Create `/etc/apache2/sites-enabled/050-conduit.conf` and copy-and-paste this:

```apache
# Requires mod_proxy and mod_proxy_http
#
# On Apache instance compiled from source,
# paste into httpd-ssl.conf or httpd.conf

Listen 8448

<VirtualHost *:443 *:8448>

ServerName your.server.name # EDIT THIS

AllowEncodedSlashes NoDecode

# TCP
ProxyPass /_matrix/ http://127.0.0.1:6167/_matrix/ timeout=300 nocanon
ProxyPassReverse /_matrix/ http://127.0.0.1:6167/_matrix/

# UNIX socket
#ProxyPass /_matrix/ unix:/run/conduit/conduit.sock|http://127.0.0.1:6167/_matrix/ nocanon
#ProxyPassReverse /_matrix/ unix:/run/conduit/conduit.sock|http://127.0.0.1:6167/_matrix/

</VirtualHost>
```

**You need to make some edits again.** When you are done, run

```bash
# Debian
$ sudo systemctl reload apache2

# Installed from source
$ sudo apachectl -k graceful
```

### Caddy

Create `/etc/caddy/conf.d/conduit_caddyfile` and enter this (substitute for your server name).

```caddy
your.server.name, your.server.name:8448 {
        # TCP
        reverse_proxy /_matrix/* 127.0.0.1:6167

        # UNIX socket
        #reverse_proxy /_matrix/* unix//run/conduit/conduit.sock
}
```

That's it! Just start or enable the service and you're set.

```bash
$ sudo systemctl enable caddy
```

### Nginx

If you use Nginx and not Apache, add the following server section inside the http section of `/etc/nginx/nginx.conf`

```nginx
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    listen 8448 ssl http2;
    listen [::]:8448 ssl http2;
    server_name your.server.name; # EDIT THIS
    merge_slashes off;

    # Nginx defaults to only allow 1MB uploads
    # Increase this to allow posting large files such as videos
    client_max_body_size 20M;

    # UNIX socket
    #upstream backend {
    #    server unix:/run/conduit/conduit.sock;
    #}

    location /_matrix/ {
        # TCP
        proxy_pass http://127.0.0.1:6167;

        # UNIX socket
        #proxy_pass http://backend;

        proxy_set_header Host $http_host;
        proxy_buffering off;
        proxy_read_timeout 5m;
    }

    ssl_certificate /etc/letsencrypt/live/your.server.name/fullchain.pem; # EDIT THIS
    ssl_certificate_key /etc/letsencrypt/live/your.server.name/privkey.pem; # EDIT THIS
    ssl_trusted_certificate /etc/letsencrypt/live/your.server.name/chain.pem; # EDIT THIS
    include /etc/letsencrypt/options-ssl-nginx.conf;
}
```

**You need to make some edits again.** When you are done, run

```bash
$ sudo systemctl reload nginx
```

## SSL Certificate

If you chose Caddy as your web proxy SSL certificates are handled automatically and you can skip this step.

The easiest way to get an SSL certificate, if you don't have one already, is to [install](https://certbot.eff.org/instructions) `certbot` and run this:

```bash
# To use ECC for the private key, 
# paste into /etc/letsencrypt/cli.ini:
# key-type = ecdsa
# elliptic-curve = secp384r1

$ sudo certbot -d your.server.name
```
[Automated renewal](https://eff-certbot.readthedocs.io/en/stable/using.html#automated-renewals) is usually preconfigured.

If using Cloudflare, configure instead the edge and origin certificates in dashboard. In case you’re already running a website on the same Apache server, you can just copy-and-paste the SSL configuration from your main virtual host on port 443 into the above-mentioned vhost.

## You're done!

Now you can start Conduit with:

```bash
$ sudo systemctl start conduit
```

Set it to start automatically when your system boots with:

```bash
$ sudo systemctl enable conduit
```

## How do I know it works?

You can open <https://app.element.io>, enter your homeserver and try to register.

You can also use these commands as a quick health check.

```bash
$ curl https://your.server.name/_matrix/client/versions

# If using port 8448
$ curl https://your.server.name:8448/_matrix/client/versions
```

- To check if your server can talk with other homeservers, you can use the [Matrix Federation Tester](https://federationtester.matrix.org/).
  If you can register but cannot join federated rooms check your config again and also check if the port 8448 is open and forwarded correctly.

# What's next?

## Audio/Video calls

For Audio/Video call functionality see the [TURN Guide](../turn.md).

## Appservices

If you want to set up an appservice, take a look at the [Appservice Guide](../appservices.md).
