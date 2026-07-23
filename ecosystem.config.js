// PM2 process config for the tx stream POC.
//
// Build first, then start with PM2:
//   go build -o tx_stream_clock_poc .
//   pm2 start ecosystem.config.js
//
// One process per block-time source (clock, event, server), running side by
// side for comparison. Values are hard-coded below — edit them directly.
//
// Handy commands:
//   pm2 logs tx_stream_clock_poc-clock    # tail one process
//   pm2 logs                              # tail all
//   pm2 restart all / pm2 stop all / pm2 delete all
//   pm2 save && pm2 startup               # persist across reboots

const endpoint = "http://10.0.0.250:10000";

const common = {
  script: "./tx_stream_clock_poc", // the compiled Go binary
  interpreter: "none", // run the binary directly, not via node
  cwd: __dirname,
  autorestart: true,
  max_restarts: 10,
  restart_delay: 3000, // wait 3s between restarts (stream reconnects)
  max_memory_restart: "500M",
  merge_logs: true,
  time: true, // prefix log lines with timestamps
};

module.exports = {
  apps: [
    {
      ...common,
      name: "tx_stream_clock_poc-clock",
      args: ["--endpoint", endpoint, "--block-time-source", "clock"],
      out_file: "./logs/tx_stream_clock_poc-clock.out.log",
      error_file: "./logs/tx_stream_clock_poc-clock.err.log",
    },
    {
      ...common,
      name: "tx_stream_clock_poc-event",
      args: ["--endpoint", endpoint, "--block-time-source", "event"],
      out_file: "./logs/tx_stream_clock_poc-event.out.log",
      error_file: "./logs/tx_stream_clock_poc-event.err.log",
    },
    {
      ...common,
      name: "tx_stream_clock_poc-server",
      args: ["--endpoint", endpoint, "--block-time-source", "server"],
      out_file: "./logs/tx_stream_clock_poc-server.out.log",
      error_file: "./logs/tx_stream_clock_poc-server.err.log",
    },
  ],
};
