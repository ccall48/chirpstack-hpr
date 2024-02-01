# Chirpstack-hpr (Chirpstack -> Helium Packet Router).
<p>
this project aims to be a simple container to leverage the helium packet config cli using python to interface between chirpstack v4 and the helium packet router cli. it's main focus is to replicate actions taken in Chirpstack v4 by a tenant to add/update/remove device euis to the helium packet router when a device is actioned on by a tenant.</p>
<p>
you will need to make sure this container connects to the same docker network or network as your chirpstack container is running on so that it can interact with it when changes are made.
</p>
<p>
max packets per skfs are set to 0 by default and updated on activation. if not set by the tenant this will automatically be set as the same as your max packets set for the route id. if you wish to set these per device by the skfs, you need to set `device -> configuration -> variables`. click on add variable, set key name to `max_copies`, set the value as an integer for the amount of packets per device you want to purchase if it is under or above the default route id max packets for the device.
</p>
<p>
if you find this useful give us a star or clone the repo and contribute.
</p>

## Helium Openlns
- Own your own helium OUI.
- Purchase your own devaddr block (min block 8) or have a NetID.
- Your own [Chirpstack](https://github.com/chirpstack/chirpstack) or [Chirpstack Docker](https://github.com/chirpstack/chirpstack-docker) instance.
- Chirpstack gateway bridges setup to accept gwmp on regions you intend on allowing to connect to your server.

## Initial Helium Setup
- [Helium Roaming Quickstart](https://docs.helium.com/iot/lorawan-roaming/#roaming-quickstart)

## To Get Started
- git clone this repository.
- create an external attachable network eg. `docker network create core-infra` to attach chirpstack-docker and chirpstack-hpr to.
- edit network section in the docker compose to attach to the same docker network created above in chirpstack-docker compose.
- rename `.env.sample` to `.env` and edit with required settings applicable to your installation. you should name the hosts needed by there docker container name and not docker IP address as this can change between restarts causing breakage.
- `docker compose up` should pull the current container from gh and start the container check for any errors. if you need to change things its usually best to recreate the containers `ctrl-c` to terminate current running container and disgard with `docker compose down --remove-orphans` make and save any changes as needed. once you have it right start docker in detached mode `docker compose up -d`


## TODO:
1. possibly migrate sql to more friendly pythonic sqlalchemy ORM.

## Completed
1. get python working with helium-crypto.rs and helium/proto to make changes directly over the wire.
thanks to groot for getting the inital rpc working for signing changes [helium-iot-config-py](https://github.com/mawdegroot/helium-iot-config-py)

2. external integrations to account for by device and by tenant dc usage. currently supports aws sqs, postgres and http (using rpc).

## Create chirpstack integration database.
- This step is no longer required, a sepreate table will be created in the regular postgres db to keep a synced record of
the helium devices.

Nevertheless to create a separate database for handling the regular postgres chirpstack integration as referenced by the
chirpstack docs.<br />
To enter the command line utility for PostgreSQL.:
```sh
docker exec -it chirpstack-postgres /bin/bash
psql -U postgres
```

Inside this prompt, execute the following queries to setup the helium_integration database. It is recommended to use a different username (role) and password.

```sql
-- create role for authentication
create role helium_integration with login password 'helium_integration';

-- create database
create database helium_integration with owner helium_integration;

-- exit psql
\q
```
