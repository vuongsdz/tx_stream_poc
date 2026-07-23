// PM2 process config for the tx stream POC.
//
// Build first, then start with PM2:
//   go build -o tx_stream_clock_poc .
//   pm2 start ecosystem.config.js
//
// Override the endpoint/key without editing this file:
//   GRPC_ENDPOINT=https://laserstream-mainnet-tyo.helius-rpc.com X_TOKEN=xxx pm2 start ecosystem.config.js
//
// Handy commands:
//   pm2 logs tx_stream_clock_poc      # tail logs
//   pm2 restart tx_stream_clock_poc   # restart
//   pm2 stop tx_stream_clock_poc      # stop
//   pm2 delete tx_stream_clock_poc    # remove from PM2
//   pm2 save && pm2 startup           # persist across reboots

const endpoint =
  process.env.GRPC_ENDPOINT ||
  "https://laserstream-mainnet-tyo.helius-rpc.com";
const apiKey = process.env.X_TOKEN || ""; // Helius API key

const args = ["--endpoint", endpoint];
if (apiKey) {
  args.push("--api-key", apiKey);
}

module.exports = {
  apps: [
    {
      name: "tx_stream_clock_poc",
      script: "./tx_stream_clock_poc", // the compiled Go binary
      interpreter: "none", // run the binary directly, not via node
      args,
      cwd: __dirname,
      autorestart: true,
      max_restarts: 10,
      restart_delay: 3000, // wait 3s between restarts (stream reconnects)
      max_memory_restart: "500M",
      merge_logs: true,
      time: true, // prefix log lines with timestamps
      out_file: "./logs/tx_stream_clock_poc.out.log",
      error_file: "./logs/tx_stream_clock_poc.err.log",
    },
  ],
};
