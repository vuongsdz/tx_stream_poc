// PM2 process config for the tx stream POC.
//
// Build first, then start with PM2:
//   go build -o tx_stream_clock_poc .
//   pm2 start ecosystem.config.js
//
// One process per subscription mode (tx, block), running side by side for
// comparison. Values are hard-coded below — edit them directly.
//
// Handy commands:
//   pm2 logs tx_stream_clock_poc_tx       # tail one process
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
      name: "tx_stream_clock_poc_tx",
      args: ["--endpoint", endpoint, "--sub-mode", "tx"],
      out_file: "./logs/tx_stream_clock_poc_tx.out.log",
      error_file: "./logs/tx_stream_clock_poc_tx.err.log",
    },
    {
      ...common,
      name: "tx_stream_clock_poc_block",
      args: ["--endpoint", endpoint, "--sub-mode", "block"],
      out_file: "./logs/tx_stream_clock_poc_block.out.log",
      error_file: "./logs/tx_stream_clock_poc_block.err.log",
    },
  ],
};
