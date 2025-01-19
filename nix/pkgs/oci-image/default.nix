{ inputs

# Dependencies
, dockerTools
, lib
, main
, stdenv
, tini
}:

dockerTools.buildLayeredImage {
  name = main.pname;
  tag = "main";
  created = "@${toString inputs.self.lastModified}";
  contents = [
    dockerTools.caCertificates
    main
  ];
  config = {
    Entrypoint = if !stdenv.hostPlatform.isDarwin
      # Use the `tini` init system so that signals (e.g. ctrl+c/SIGINT)
      # are handled as expected
      then [ "${lib.getExe' tini "tini"}" "--" ]
      else [];
    Cmd = [
      "${lib.getExe main}"
    ];
    Env = [
      "RUST_BACKTRACE=full"
    ];
    Labels = {
      "org.opencontainers.image.title" = main.pname;
      "org.opencontainers.image.version" = main.version;
      "org.opencontainers.image.revision" = inputs.self.rev or inputs.self.dirtyRev or "";
      # "org.opencontainers.image.created" = builtins.formatTime "%Y-%m-%dT%H:%M:%SZ" inputs.self.lastModified;
    };
  };
}
