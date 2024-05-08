# conduwuit for Docker

## Docker

To run conduwuit with Docker you can either build the image yourself or pull it from a registry.


### Use a registry

OCI images for conduwuit are available in the registries listed below.

| Registry        | Image                                                           | Size                          | Notes                  |
| --------------- | --------------------------------------------------------------- | ----------------------------- | ---------------------- |
| GitHub Registry | [ghcr.io/girlbossceo/conduwuit:latest][gh] | ![Image Size][shield-latest]  | Stable tagged image.          |
| GitLab Registry | [registry.gitlab.com/conduwuit/conduwuit:latest][gl] | ![Image Size][shield-latest]  | Stable tagged image.          |
| Docker Hub      | [docker.io/girlbossceo/conduwuit:latest][dh]             | ![Image Size][shield-latest]  | Stable tagged image.          |
| GitHub Registry | [ghcr.io/girlbossceo/conduwuit:main][gh]   | ![Image Size][shield-main]    | Stable main branch.   |
| GitLab Registry | [registry.gitlab.com/conduwuit/conduwuit:main][gl]   | ![Image Size][shield-main]    | Stable main branch.   |
| Docker Hub      | [docker.io/girlbossceo/conduwuit:main][dh]               | ![Image Size][shield-main]    | Stable main branch.   |

[dh]: https://hub.docker.com/repository/docker/girlbossceo/conduwuit
[gh]: https://github.com/girlbossceo/conduwuit/pkgs/container/conduwuit
[gl]: https://gitlab.com/conduwuit/conduwuit/container_registry/6351657
[shield-latest]: https://img.shields.io/docker/image-size/girlbossceo/conduwuit/latest
[shield-main]: https://img.shields.io/docker/image-size/girlbossceo/conduwuit/main


Use
```bash
docker image pull <link>
```
to pull it to your machine.


### Run

When you have the image you can simply run it with

```bash
docker run -d -p 8448:6167 \
  -v db:/var/lib/conduwuit/ \
  -e CONDUIT_SERVER_NAME="your.server.name" \
  -e CONDUIT_DATABASE_BACKEND="rocksdb" \
  -e CONDUIT_ALLOW_REGISTRATION=false \
  -e CONDUIT_ALLOW_FEDERATION=true \
  -e CONDUIT_MAX_REQUEST_SIZE="40000000" \
  -e CONDUIT_TRUSTED_SERVERS="[\"matrix.org\"]" \
  -e CONDUIT_LOG="warn,ruma_state_res=warn" \
  --name conduit <link>
```

or you can use [docker compose](#docker-compose).

The `-d` flag lets the container run in detached mode. You may supply an optional `conduwuit.toml` config file, an example can be found [here](../configuration.md).
You can pass in different env vars to change config values on the fly. You can even configure conduwuit completely by using env vars. For an overview of possible
values, please take a look at the `docker-compose.yml` file.

If you just want to test conduwuit for a short time, you can use the `--rm` flag, which will clean up everything related to your container after you stop it.

### Docker-compose

If the `docker run` command is not for you or your setup, you can also use one of the provided `docker-compose` files.

Depending on your proxy setup, you can use one of the following files;
- If you already have a `traefik` instance set up, use [`docker-compose.for-traefik.yml`](docker-compose.for-traefik.yml)
- If you don't have a `traefik` instance set up (or any other reverse proxy), use [`docker-compose.with-traefik.yml`](docker-compose.with-traefik.yml)
- For any other reverse proxy, use [`docker-compose.yml`](docker-compose.yml)

When picking the traefik-related compose file, rename it so it matches `docker-compose.yml`, and
rename the override file to `docker-compose.override.yml`. Edit the latter with the values you want
for your server.

Additional info about deploying conduwuit can be found [here](generic.md).

### Build

To build the conduwuit image with docker-compose, you first need to open and modify the `docker-compose.yml` file. There you need to comment the `image:` option and uncomment the `build:` option. Then call docker compose with:

```bash
docker compose up
```

This will also start the container right afterwards, so if want it to run in detached mode, you also should use the `-d` flag.

### Run

If you already have built the image or want to use one from the registries, you can just start the container and everything else in the compose file in detached mode with:

```bash
docker compose up -d
```

> **Note:** Don't forget to modify and adjust the compose file to your needs.

### Use Traefik as Proxy

As a container user, you probably know about Traefik. It is a easy to use reverse proxy for making
containerized app and services available through the web. With the two provided files,
[`docker-compose.for-traefik.yml`](docker-compose.for-traefik.yml) (or
[`docker-compose.with-traefik.yml`](docker-compose.with-traefik.yml)) and
[`docker-compose.override.yml`](docker-compose.override.yml), it is equally easy to deploy
and use conduwuit, with a little caveat. If you already took a look at the files, then you should have
seen the `well-known` service, and that is the little caveat. Traefik is simply a proxy and
loadbalancer and is not able to serve any kind of content, but for conduwuit to federate, we need to
either expose ports `443` and `8448` or serve two endpoints `.well-known/matrix/client` and
`.well-known/matrix/server`.

With the service `well-known` we use a single `nginx` container that will serve those two files.

So...step by step:

1. Copy [`docker-compose.for-traefik.yml`](docker-compose.for-traefik.yml) (or
[`docker-compose.with-traefik.yml`](docker-compose.with-traefik.yml)) and [`docker-compose.override.yml`](docker-compose.override.yml) from the repository and remove `.for-traefik` (or `.with-traefik`) from the filename.
2. Open both files and modify/adjust them to your needs. Meaning, change the `CONDUIT_SERVER_NAME` and the volume host mappings according to your needs.
3. Create the `conduwuit.toml` config file, an example can be found [here](../configuration.md), or set `CONDUIT_CONFIG=""` and configure conduwuit per env vars.
4. Uncomment the `element-web` service if you want to host your own Element Web Client and create a `element_config.json`.
5. Create the files needed by the `well-known` service.

   - `./nginx/matrix.conf` (relative to the compose file, you can change this, but then also need to change the volume mapping)

     ```nginx
     server {
         server_name <SUBDOMAIN>.<DOMAIN>;
         listen      80 default_server;

         location /.well-known/matrix/server {
            return 200 '{"m.server": "<SUBDOMAIN>.<DOMAIN>:443"}';
            types { } default_type "application/json; charset=utf-8";
         }

        location /.well-known/matrix/client {
            return 200 '{"m.homeserver": {"base_url": "https://<SUBDOMAIN>.<DOMAIN>"}}';
            types { } default_type "application/json; charset=utf-8";
            add_header "Access-Control-Allow-Origin" *;
        }

        location / {
            return 404;
        }
     }
     ```

6. Run `docker compose up -d`
7. Connect to your homeserver with your preferred client and create a user. You should do this immediately after starting Conduit, because the first created user is the admin.




## Voice communication

In order to make or receive calls, a TURN server is required. conduwuit suggests using [Coturn](https://github.com/coturn/coturn) for this purpose, which is also available as a Docker image. Before proceeding with the software installation, it is essential to have the necessary configurations in place.

### Configuration

Create a configuration file called `coturn.conf` containing:

```conf
use-auth-secret
static-auth-secret=<a secret key>
realm=<your server domain>
```
A common way to generate a suitable alphanumeric secret key is by using `pwgen -s 64 1`.

These same values need to be set in conduwuit. You can either modify conduwuit.toml to include these lines:
```
turn_uris = ["turn:<your server domain>?transport=udp", "turn:<your server domain>?transport=tcp"]
turn_secret = "<secret key from coturn configuration>"
```
or append the following to the docker environment variables dependig on which configuration method you used earlier:
```yml
CONDUIT_TURN_URIS: '["turn:<your server domain>?transport=udp", "turn:<your server domain>?transport=tcp"]'
CONDUIT_TURN_SECRET: "<secret key from coturn configuration>"
```
Restart Conduit to apply these changes.

### Run
Run the [Coturn](https://hub.docker.com/r/coturn/coturn) image using
```bash
docker run -d --network=host -v $(pwd)/coturn.conf:/etc/coturn/turnserver.conf coturn/coturn
```

or docker-compose. For the latter, paste the following section into a file called `docker-compose.yml`
and run `docker compose up -d` in the same directory.

```yml
version: 3
services:
    turn:
      container_name: coturn-server
      image: docker.io/coturn/coturn
      restart: unless-stopped
      network_mode: "host"
      volumes:
        - ./coturn.conf:/etc/coturn/turnserver.conf
```

To understand why the host networking mode is used and explore alternative configuration options, please visit the following link: https://github.com/coturn/coturn/blob/master/docker/coturn/README.md.
For security recommendations see Synapse's [Coturn documentation](https://github.com/matrix-org/synapse/blob/develop/docs/setup/turn/coturn.md#configuration).
