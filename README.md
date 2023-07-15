# Chirpstack-hpr (Chirpstack -> Helium Packet Router).
This project aims to be a simple container to leverage the helium packet config cli using python to interface between chirpstack v4 and the helium packet router cli. It's main focus at the moment is to replicate actions taken in Chirpstack v4 by a tenant to add/update/remove device euis to the helium packet router when updated by a tenant.

## Milestone 1
add device to OUI users route-id using the hpr connector when device added to a tenants console in chirpstack.

## Milestone 2
update device to OUI users route-id using the hpr connector when a device is enabled or disabled in chirpstack.
1. enable = device on route-id
2. disabled = device removed from route-id

## Milestone 3
<p>remove device to OUI users route-id using the hpr connector when a tenant deletes a device in chirpstack.</p>
***TODO:*** when a device is removed from chirpstack before we can do a lookup for it using the chirpstack backend grpc,
thinking of adding a database to the exisiting chirpstack postgres to store some basic information that can be recalled
when handling a device deletion.

## Milestone 4
work on assigning tenants DC and the workings of subtracting according the uplinks and downlinks heard for devices connected.<br />
auto set devices to go into disabled mode when a tenants DC reaches a pre set threshold.

## TODO: Creating helium database WIP
You must create a separate database for handling the Helium integration. To enter the command line utility for PostgreSQL:
```sh
sudo -u postgres psql
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
