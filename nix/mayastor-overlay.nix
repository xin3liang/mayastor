self: super: {

  fio = super.callPackage ./pkgs/fio { };
  images = super.callPackage ./pkgs/images { };
  libiscsi = super.callPackage ./pkgs/libiscsi { };
  libspdk = (super.callPackage ./pkgs/libspdk { }).release;
  libspdk-dev = (super.callPackage ./pkgs/libspdk { }).debug;
  mayastor = (super.callPackage ./pkgs/mayastor { }).release;
  mayastor-adhoc = (super.callPackage ./pkgs/mayastor { }).adhoc;
  mayastor-dev = (super.callPackage ./pkgs/mayastor { }).debug;
  mayastor-cov = (super.callPackage ./pkgs/mayastor { }).cov;
  mkContainerEnv = super.callPackage ./lib/mkContainerEnv.nix { };
  ms-buildenv = super.callPackage ./pkgs/ms-buildenv { };
  nvmet-cli = super.callPackage ./pkgs/nvmet-cli { };
  units = (super.callPackage ./pkgs/mayastor/units.nix { });
}
