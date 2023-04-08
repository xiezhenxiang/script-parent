package ai.plantdata.script.util.other;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author xiezhenxiang 2023/3/20
 */
public class CollectionUtil {

    public static <T> boolean isEmpty(Collection<T> ls) {
        return ls == null || ls.isEmpty();
    }

    public static <T> boolean isNotEmpty(Collection<T> ls) {
        return !isEmpty(ls);
    }

    public static <T> void splitExec(List<T> ls, Integer batchSize, Consumer<List<T>> consumer) {
        if (ls == null || ls.isEmpty()) {
            return;
        }
        if (ls.size() <= batchSize) {
            consumer.accept(ls);
            return;
        }
        int index = 0;
        while (index < ls.size()) {
            int endIndex = Math.min(index + batchSize, ls.size());
            consumer.accept(ls.subList(index, endIndex));
            index += batchSize;
        }
    }
}
