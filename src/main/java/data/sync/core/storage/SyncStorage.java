package data.sync.core.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * 只用来测试，只用来测试，只用来测试
 * Created by hesiyuan on 15/6/23.
 */
public class SyncStorage extends Storage{
    List<Line> list = new ArrayList<Line>();
    int index = 0;
    int size = 0;
    @Override
    public String info() {
        return null;
    }

    @Override
    public void setPushClosed(boolean close) {
        super.setPushClosed(close);
        size = list.size();
        System.out.println(size);
    }

    @Override
    public boolean push(Line line) {
        list.add(line);
        return true;
    }

    @Override
    public boolean push(Line[] lines, int size) {
        return false;
    }

    @Override
    public boolean fakePush(int lineLength) {
        return false;
    }

    @Override
    public Line pull() {
        if(index<size) {
            index++;

            return list.get(index - 1);
        }else{
            return null;
        }
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

    @Override
    public int getLineLimit() {
        return 0;
    }
}
