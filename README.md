# Chirpstack-hpr (Chirpstack -> Helium Packet Router).
<p>
this project aims to be a simple container to leverage the helium packet config cli using python to interface between chirpstack v4 and the helium packet router cli. it's main focus at the moment is to replicate actions taken in Chirpstack v4 by a tenant to add/update/remove device euis to the helium packet router when a device is actioned by a tenant.</p>
<p>
you will need to make sure this container connects to the same network the container your chirpstack instance is running on so that it can interact with it when changes are made.
</p>

## Helium Openlns
- Own your own helium OUI.
- Purchase your own devaddr block (min block 8) or have a NetID.
- Your own [Chirpstack](https://github.com/chirpstack/chirpstack) instance.
- Chirpstack gateway bridges setup to accept gwmp on regions you intend on allowing to connect to your server.

## Initial Helium Setup
- [Helium Roaming Quickstart](https://docs.helium.com/iot/lorawan-roaming/#roaming-quickstart)

## Create chirpstack integration database.
You must create a separate database for handling the chirpstack integration.<br />
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

## TODO: Creating helium database WIP
this may not be required, for now just created a few extra tables in the main chirpstack database.

### Milestone 1 [done]
add device to OUI users route-id using the hpr connector when device added to a tenants console in chirpstack.

### Milestone 2 [done]
update device to OUI users route-id using the hpr connector when a device is enabled or disabled in chirpstack.
1. enable = device on route-id
2. disabled = device removed from route-id

### Milestone 3 [done]
<p>remove device to OUI users route-id using the hpr connector when a tenant deletes a device in chirpstack.</p>
***TODO:*** when a device is removed from chirpstack before we can do a lookup for it using the chirpstack backend grpc,
thinking of adding a database to the exisiting chirpstack postgres to store some basic information that can be recalled
when handling a device deletion.

### Milestone 4
1. work on assigning tenants DC and the workings of subtracting according the uplinks heard for devices connected.<br />
2. auto set devices to go into disabled mode when a tenants DC reaches a pre set threshold.

### Milestone 5
<p>get python working with helium-crypto.rs and helium/proto to make changes directly over the wire, i'm currently
unable to get this function working correctly on the helium side. Any help here would be appreciated.</p>
