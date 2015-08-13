package data.sync.core.storage;

/**
 * Created by hesiyuan on 15/7/10.
 */
public class FileStorage extends Storage {
    @Override
    public boolean push(Line line) {
        return false;
    }

    @Override
    public boolean push(Line[] lines) {
        return false;
    }

    @Override
    public Line pull() {
        return null;
    }

    @Override
    public int pull(Line[] lines) {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean empty() {
        return false;
    }
}
