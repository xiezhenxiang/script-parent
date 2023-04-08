package ai.plantdata.script.util.other;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;

/**
 * 系统命令行工具
 * @author xiezhenxiang 2020/11/27
 **/
public class CommandUtil {

    public static String execCommand(String command, String... args) {

        CommandLine commandLine = CommandLine.parse(command);
        commandLine.addArguments(args);
        DefaultExecutor executor = new DefaultExecutor();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
        executor.setStreamHandler(streamHandler);
        String returnStr;
        try {
            executor.execute(commandLine);
            returnStr = outputStream.toString("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return returnStr;
    }

    public static void main(String[] args) {
        System.out.println(execCommand("java -version"));
    }


}
