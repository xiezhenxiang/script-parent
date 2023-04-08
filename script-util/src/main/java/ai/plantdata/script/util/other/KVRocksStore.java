package ai.plantdata.script.util.other;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Bovin
 * @description
 * @since 2020-07-23 14:17
 **/
public class KVRocksStore {

    private final static int COUNT = 1024 * 1024;

    private final RocksDB rocksDBs;

    public KVRocksStore(String dir) throws RocksDBException {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            options.setWriteBufferSize(COUNT * 256)
                    .setMaxWriteBufferNumber(4)
                    .setMinWriteBufferNumberToMerge(2)
                    .setMaxOpenFiles(-1)
                    .setAllowMmapReads(true)
                    .setAllowMmapWrites(false)
                    .setRandomAccessMaxBufferSize(SstFileManager.BYTES_MAX_DELETE_CHUNK_DEFAULT)
                    .setStatsHistoryBufferSize(SstFileManager.BYTES_MAX_DELETE_CHUNK_DEFAULT)
                    .setTargetFileSizeBase(SstFileManager.BYTES_MAX_DELETE_CHUNK_DEFAULT * 16);
            this.rocksDBs = RocksDB.open(options, dir);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return rocksDBs.get(key);
        } catch (RocksDBException e) {
            return null;
        }
    }

    public List<byte[]> get(List<byte[]> keys) {

        List<byte[]> ls = new ArrayList<>();
        try {
            int cnt = 0;
            while (cnt < keys.size()) {
                if (cnt + 10000 >= keys.size()) {
                    if (cnt == 0) {
                        ls.addAll(rocksDBs.multiGetAsList(keys));
                    } else {
                        ls.addAll(rocksDBs.multiGetAsList(keys.subList(cnt, keys.size())));
                    }
                } else{
                    ls.addAll(rocksDBs.multiGetAsList(keys.subList(cnt, cnt + 10000)));
                }
                cnt += 10000;
            }
        } catch (RocksDBException e) {
            ls = null;
        }
        return ls;
    }

    public void put(byte[] key, byte[] value) {
        try {
            rocksDBs.put(key, value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void close() throws Exception {
        rocksDBs.close();
    }
}
