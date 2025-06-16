package site.hnfy258.aof.utils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public class FileUtils {
    public static void safeRenameFile(File source, File target){
        try{
            atomicMoveWithFallback(source.toPath(), target.toPath());
        }catch(Exception e){
            throw new RuntimeException("Failed to rename file", e);
        }
    }

    public static File createBackupFile(File file,String backupSuffix) throws IOException {
        if(!file.exists()){
            return null;
        }
        File backupFile = new File(file.getAbsolutePath() + backupSuffix);
        if(backupFile.exists()){
            backupFile.delete();
        }

        atomicMoveWithFallback(file.toPath(), backupFile.toPath());
        return backupFile;
    }

    private static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        try{
            Files.move(source,target, StandardCopyOption.ATOMIC_MOVE);
        }catch(Exception e){
            try{
                Files.move(source,target,StandardCopyOption.REPLACE_EXISTING);
            }catch(Exception ex){
                throw new RuntimeException("Failed to rename file", ex);
            }
        }
        finally {
            flushDir(target.toAbsolutePath().normalize().getParent());
        }
    }

    private static void flushDir(Path path)  throws IOException {
        String os = System.getProperty("os.name").toLowerCase();
        if(path !=null && !os.contains("win")&& !os.contains("z/os")){
            try(FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)){
                dir.force(true);
            }
        }
    }

}
