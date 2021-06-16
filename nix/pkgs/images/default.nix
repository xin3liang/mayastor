# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.
#
# We limit max number of image layers to 42 because there is a bug in
# containerd triggered when there are too many layers:
# https://github.com/containerd/containerd/issues/4684

{ stdenv
, busybox
, dockerTools
, e2fsprogs
, git
, lib
, writeScriptBin
, xfsprogs
, mayastor
, mayastor-dev
, mayastor-adhoc
, utillinux
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  env = lib.makeBinPath [ busybox xfsprogs e2fsprogs utillinux ];

  # common props for all mayastor images
  mayastorImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
  mayastorCsiImageProps = {
    tag = version;
    created = "now";
    config = {
      Entrypoint = [ "/bin/mayastor-csi" ];
      Env = [ "PATH=${env}" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
  clientImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
  mayastorIscsiadm = writeScriptBin "mayastor-iscsiadm" ''
    #!${stdenv.shell}
    chroot /host /usr/bin/env -i PATH="/sbin:/bin:/usr/bin" iscsiadm "$@"
  '';
in
{
  mayastor = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor";
    contents = [ busybox mayastor ];
  });

  mayastor-dev = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor-dev";
    contents = [ busybox mayastor-dev ];
  });

  mayastor-adhoc = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor-adhoc";
    contents = [ busybox mayastor-adhoc ];
  });

  # The algorithm for placing packages into the layers is not optimal.
  # There are a couple of layers with negligible size and then there is one
  # big layer with everything else. That defeats the purpose of layering.
  mayastor-csi = dockerTools.buildLayeredImage (mayastorCsiImageProps // {
    name = "mayadata/mayastor-csi";
    contents = [ busybox mayastor mayastorIscsiadm ];
    maxLayers = 42;
  });

  mayastor-csi-dev = dockerTools.buildImage (mayastorCsiImageProps // {
    name = "mayadata/mayastor-csi-dev";
    contents = [ busybox mayastor-dev mayastorIscsiadm ];
  });

  mayastor-client = dockerTools.buildImage (clientImageProps // {
    name = "mayadata/mayastor-client";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/mayastor-client" ]; };
  });
}
