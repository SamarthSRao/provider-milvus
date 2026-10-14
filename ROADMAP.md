# Milvus Provider Roadmap

The [issue tracker](https://github.com/openeverest/provider-milvus/issues) is the source of
truth; this page summarizes it. What works today is listed under
[Capabilities](README.md#capabilities).

## Done

| Area | Issues |
|---|---|
| Configuration, validation and bundled-dependency sizing | [#3](https://github.com/openeverest/provider-milvus/issues/3), [#4](https://github.com/openeverest/provider-milvus/issues/4) |
| External etcd and object storage | [#5](https://github.com/openeverest/provider-milvus/issues/5) |
| Status, connection details and credentials (generation, rotation) | [#6](https://github.com/openeverest/provider-milvus/issues/6), [#7](https://github.com/openeverest/provider-milvus/issues/7), [#8](https://github.com/openeverest/provider-milvus/issues/8) |
| Backups (on demand and scheduled) and restore | [#9](https://github.com/openeverest/provider-milvus/issues/9), [#10](https://github.com/openeverest/provider-milvus/issues/10) |
| Prometheus PodMonitor | [#11](https://github.com/openeverest/provider-milvus/issues/11) |
| MixCoord active-standby, deployment groups, Woodpecker | [#17](https://github.com/openeverest/provider-milvus/issues/17), [#18](https://github.com/openeverest/provider-milvus/issues/18), [#19](https://github.com/openeverest/provider-milvus/issues/19) |
| GPU images, per-component images and pod customization | [#49](https://github.com/openeverest/provider-milvus/issues/49), [#50](https://github.com/openeverest/provider-milvus/issues/50) |
| Unit tests | [#21](https://github.com/openeverest/provider-milvus/issues/21) |

## In progress

| Area | Remaining | Issue |
|---|---|---|
| Version management | Compatibility matrix validation | [#15](https://github.com/openeverest/provider-milvus/issues/15) |
| Integration tests | Component scaling, dependency verification | [#22](https://github.com/openeverest/provider-milvus/issues/22) |
| End-to-end tests | Scaling and backup/restore workflows | [#23](https://github.com/openeverest/provider-milvus/issues/23) |
| Example manifests | Multi-AZ, monitoring and logging | [#27](https://github.com/openeverest/provider-milvus/issues/27) |

## Planned

| Area | Issue |
|---|---|
| Health checks: probe configuration, component health | [#12](https://github.com/openeverest/provider-milvus/issues/12) |
| Logging: level, destinations, OpenEverest integration | [#13](https://github.com/openeverest/provider-milvus/issues/13) |
| Safe horizontal scaling of data and query nodes | [#14](https://github.com/openeverest/provider-milvus/issues/14) |
| Maintenance windows and approval gates | [#16](https://github.com/openeverest/provider-milvus/issues/16) |
| CDC | [#20](https://github.com/openeverest/provider-milvus/issues/20) |
| Conformance tests | [#24](https://github.com/openeverest/provider-milvus/issues/24) |
| Architecture documentation and usage guide | [#25](https://github.com/openeverest/provider-milvus/issues/25), [#26](https://github.com/openeverest/provider-milvus/issues/26) |
| Performance and resource efficiency | [#28](https://github.com/openeverest/provider-milvus/issues/28), [#29](https://github.com/openeverest/provider-milvus/issues/29) |

## Exploratory

| Area | Issue |
|---|---|
| Multi-tenancy, disaster recovery, network policies, Helm values passthrough, operator versioning, GitOps | [#30](https://github.com/openeverest/provider-milvus/issues/30) |
| Advertise a secondary-network (SR-IOV) address | [#51](https://github.com/openeverest/provider-milvus/issues/51) |
| Attu web UI | [#52](https://github.com/openeverest/provider-milvus/issues/52) |
