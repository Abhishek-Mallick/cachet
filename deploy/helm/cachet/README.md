# Cachet Helm chart

```bash
helm install cachet oci://ghcr.io/abhishek-mallick/charts/cachet \
  --version 0.1.0 \
  --values my-values.yaml
```

## What this chart does and does not deploy

Cachet's default topology is a **sidecar over a Unix socket**. It is what the benchmarks measure and
roughly 15% better at p99 than a shared service tier, because there is no network hop on the read
path.

A chart cannot deploy a sidecar: it belongs to *your* Deployment, not to this release. So:

| `topology` | The engine | Everything else |
|---|---|---|
| `sidecar` *(default)* | Not deployed. Include the `cachet.sidecar` template in your own pod spec | Tailer and optional verifier deployed normally |
| `service` | Deployed as a Deployment behind a ClusterIP Service | Same |

### Using the sidecar

```yaml
spec:
  template:
    spec:
      containers:
        - name: your-app
          volumeMounts:
            - name: cachet-socket
              mountPath: /var/run/cachet
        {{- include "cachet.sidecar" (dict "root" .) | nindent 12 }}
      volumes:
        - name: cachet-socket
          emptyDir: {}
        - name: cachet-config
          configMap:
            name: cachet
```

Then `cachet.Dial(ctx, "unix:///var/run/cachet/cachet.sock")`.

## Things this chart is opinionated about

**The tailer is a StatefulSet with exactly one replica.** Two tailers on one shard share a
replication id and fight over the connection, and the resulting failure looks like a CDC bug rather
than a collision. Checkpoints are persisted by default: a tailer that restarts without its
checkpoint resumes from the *current* binlog position and silently skips every write made while it
was down.

**Images are referenced by digest when you provide one, and the digest wins over the tag.** Images
are signed by digest, and a tag can be moved after signing. The chart warns on install if no digest
is pinned.

**Credentials come from a Secret you create.** They are never rendered into the ConfigMap, which is
readable by anything that can list ConfigMaps and appears in `helm get values` output.

```yaml
existingSecret: cachet-db
secretKeys: { username: username, password: password }
```

**Run the verifier before you route traffic.** `sextant.enabled=true` with `sextant.shadow=true`
reads and checks without serving, so you get your own consistency numbers before anything depends
on them.

## Values

See [`values.yaml`](./values.yaml) — every value is commented with why it defaults the way it does.
