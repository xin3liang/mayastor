{ stdenv
, clang_11
, dockerTools
, e2fsprogs
, lib
, libaio
, libbsd
, libspdk
, libspdk-dev
, libpcap
, libudev
, liburing
, makeRustPlatform
, numactl
, openssl
, pkg-config
, protobuf
, sources
, xfsprogs
, utillinux
, llvmPackages_11
, targetPackages
, buildPackages
, targetPlatform
, version
, cargoBuildFlags ? [ ]
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable;
    cargo = channel.stable;
  };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  src_list = [
    ".git"
    "Cargo.lock"
    "Cargo.toml"
    "cli"
    "composer"
    "csi"
    "devinfo"
    "jsonrpc"
    "mayastor"
    "mbus-api"
    "nvmeadm"
    "rpc"
    "spdk-rs"
    "sysfs"
  ];
  buildProps = rec {
    name = "mayastor";
    inherit version cargoBuildFlags;
    src = whitelistSource ../../../. src_list;
    LIBCLANG_PATH = "${llvmPackages_11.libclang.lib}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    nativeBuildInputs = [ pkg-config protobuf llvmPackages_11.clang ];
    buildInputs = [
      llvmPackages_11.libclang
      protobuf
      libaio
      libbsd
      libpcap
      libudev
      liburing
      numactl
      openssl
      utillinux
    ];
    cargoLock = {
      lockFile = ../../../Cargo.lock;
      outputHashes = {
        "h2-0.3.3" = "sha256-Y4AaBj10ZOutI37sVRY4yVUYmVWj5dwPbPhBhPWHNiQ=";
        "nats-0.15.2" = "sha256:1whr0v4yv31q5zwxhcqmx4qykgn5cgzvwlaxgq847mymzajpcsln";
      };
    };
    doCheck = false;
    meta = { platforms = lib.platforms.linux; };
    fixupPhase = ''
      echo "fixing rpaths in mayastor binaries to point to $out/lib"
      patchelf \
          --set-interpreter "$(cat $NIX_CC/nix-support/dynamic-linker)" \
          --set-rpath $out/lib \
          $out/bin/mayastor
    '';
  };
in
{
  release = rustPlatform.buildRustPackage (buildProps // {
    cargoBuildFlags = "--bin mayastor --bin mayastor-client --bin mayastor-csi";
    buildType = "release";
    buildInputs = buildProps.buildInputs ++ [ libspdk ];
    SPDK_PATH = "${libspdk}";
  });
  debug = rustPlatform.buildRustPackage (buildProps // {
    buildType = "debug";
    buildInputs = buildProps.buildInputs ++ [ libspdk-dev ];
    SPDK_PATH = "${libspdk-dev}";
  });
}
