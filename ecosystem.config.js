// PM2 process config for the tx stream POC.
//
// Build first, then start with PM2:
//   go build -o tx_stream_clock_poc .
//   go build -o blockcompare ./cmd/blockcompare
//   go build -o blockobserver ./cmd/blockobserver
//   pm2 start ecosystem.config.js
//
// - tx_stream_clock_poc_*: one process per (sub-mode × commitment), swap → MQTT.
// - blockcompare: streams blocks at processed + confirmed → Kafka.
// - blockobserver: consumes Kafka → Mongo (upsert per slot) for the
//   processed→confirmed comparison.
// Values are hard-coded below — edit them directly.
//
// Handy commands:
//   pm2 logs tx_stream_clock_poc_tx_confirmed   # tail one process
//   pm2 logs                                    # tail all
//   pm2 restart all / pm2 stop all / pm2 delete all
//   pm2 save && pm2 startup                     # persist across reboots

const endpoint = "http://10.0.0.250:10000";
const kafkaBrokers = "10.0.0.230:19092,10.0.0.231:19092,10.0.0.232:19092"; // TODO: set real Kafka broker(s)
const mongoUri = "mongodb://10.0.0.29:27017/stream"; // TODO: set real Mongo URI
const kafkaTopic = "block_obs";

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
      name: "tx_stream_clock_poc_tx_processed",
      args: ["--endpoint", endpoint, "--sub-mode", "tx", "--commitment", "processed"],
      out_file: "./logs/tx_stream_clock_poc_tx_processed.out.log",
      error_file: "./logs/tx_stream_clock_poc_tx_processed.err.log",
    },
    {
      ...common,
      name: "tx_stream_clock_poc_tx_confirmed",
      args: ["--endpoint", endpoint, "--sub-mode", "tx", "--commitment", "confirmed"],
      out_file: "./logs/tx_stream_clock_poc_tx_confirmed.out.log",
      error_file: "./logs/tx_stream_clock_poc_tx_confirmed.err.log",
    },
    {
      ...common,
      name: "tx_stream_clock_poc_block_processed",
      args: ["--endpoint", endpoint, "--sub-mode", "block", "--commitment", "processed"],
      out_file: "./logs/tx_stream_clock_poc_block_processed.out.log",
      error_file: "./logs/tx_stream_clock_poc_block_processed.err.log",
    },
    {
      ...common,
      name: "tx_stream_clock_poc_block_confirmed",
      args: ["--endpoint", endpoint, "--sub-mode", "block", "--commitment", "confirmed"],
      out_file: "./logs/tx_stream_clock_poc_block_confirmed.out.log",
      error_file: "./logs/tx_stream_clock_poc_block_confirmed.err.log",
    },
    {
      ...common,
      script: "./blockcompare",
      name: "blockcompare",
      args: [
        "--endpoint", endpoint,
        "--kafka-brokers", kafkaBrokers,
        "--kafka-topic", kafkaTopic,
      ],
      out_file: "./logs/blockcompare.out.log",
      error_file: "./logs/blockcompare.err.log",
    },
    {
      ...common,
      script: "./blockobserver",
      name: "blockobserver",
      args: [
        "--kafka-brokers", kafkaBrokers,
        "--kafka-topic", kafkaTopic,
        "--mongo-uri", mongoUri,
      ],
      out_file: "./logs/blockobserver.out.log",
      error_file: "./logs/blockobserver.err.log",
    },
  ],
};
