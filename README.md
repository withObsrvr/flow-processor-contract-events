# flow-processor-contract-events

A Flow processor plugin for contract events.

## Building with Nix

This project uses Nix for reproducible builds.

### Prerequisites

- [Nix package manager](https://nixos.org/download.html) with flakes enabled

### Building

1. Clone the repository:
```bash
git clone https://github.com/withObsrvr/flow-processor-contract-events.git
cd flow-processor-contract-events
```

2. Build with Nix:
```bash
nix build
```

The built plugin will be available at `./result/lib/flow-processor-contract-events.so`.

### Development

To enter a development shell with all dependencies:
```bash
nix develop
```

This will automatically vendor dependencies if needed and provide a shell with all necessary tools.

### Manual Build (Inside Nix Shell)

Once in the development shell, you can manually build the plugin:
```bash
go mod tidy
go mod vendor
go build -buildmode=plugin -o flow-processor-contract-events.so .
```

## Troubleshooting

### Plugin Version Compatibility

Make sure the plugin is built with the exact same Go version that Flow uses. If you see an error like "plugin was built with a different version of package internal/goarch", check that your Go version matches the one used by the Flow application.

### CGO Required

Go plugins require CGO to be enabled. The Nix build and development shell handle this automatically, but if building manually outside of Nix, ensure you've set:
```bash
export CGO_ENABLED=1
```

### Vendoring Dependencies

For reliable builds, we recommend using vendored dependencies:
```bash
go mod vendor
git add vendor
```