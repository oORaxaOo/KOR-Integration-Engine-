# KOR Engine Stack Installation

The KOR stack is distributed as a Hermes plugin pack. It pins all four repositories to full immutable Git commit SHAs: KIE, Critical Thinking, Reliability, and Emotion.

The pack contains no credentials and does not seed local filesystem paths. Each engine remains a separate Hermes plugin and is installed through Hermes' normal installer and consent/security checks.

## Install

From a checkout of this repository, inspect `hermes-pack.yaml` and run:

```bash
hermes plugins pack show ./hermes-pack.yaml
hermes plugins pack install ./hermes-pack.yaml
```

If the installed Hermes build does not provide `hermes plugins pack`, do not attempt an improvised install from plugin import code. Use a release that supports plugin packs or the documented pinned-install fallback. KIE must never clone repositories, install packages, or edit Hermes configuration during `register()`.

## After installation

Installation and activation are separate. Confirm all four plugins appear in `hermes plugins`, then enable them through Hermes' normal plugin controls. Configure the same absolute `kie_event_socket` path for the three engine publishers and `engine_event_socket` for KIE, plus the KIE database path. Restart Hermes after configuration changes.

The pack does not configure local paths because those differ by machine/profile. It does not start KIE ingress until required settings are present.

## Version updates

Update the four commit pins together only after the repositories pass their own CI and a cross-repository compatibility check. This file defines a tested stack, not arbitrary latest commits.
