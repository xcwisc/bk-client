mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_1' '10000' '51200' '30'" &
mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_2' '10000' '51200' '30'" &
mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_3' '10000' '51200' '30'" &
mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_4' '10000' '51200' '30'" &

# mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_1' '10' '10' '30'" &
# mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_2' '10' '10' '30'" &
# mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_3' '10' '10' '30'" &
# mvn exec:java -Dexec.mainClass=bookkeeper.SingleThreadedSyncWriter -Dexec.args="'parallel4_4ledgers_sync_10000_50KB_4' '10' '10' '30'" &