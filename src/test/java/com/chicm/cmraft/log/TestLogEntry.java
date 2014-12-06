package com.chicm.cmraft.log;

import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftEntry;

public class TestLogEntry {
  private static final String FILE_NAME="d:/tmp/test";
  
  @Test
  public void testSerialization() throws Exception {
    LogEntry le1 = new LogEntry(1, 2, "key1".getBytes(), "value1".getBytes(), LogMutationType.SET);
    LogEntry le2 = new LogEntry(2, 2, "key2".getBytes(), "value2".getBytes(), LogMutationType.SET);
    LogEntry le3 = new LogEntry(3, 2, "key3".getBytes(), "value3".getBytes(), LogMutationType.SET);
    
    File f = new File(FILE_NAME);
    boolean append = f.exists();
    ObjectOutputStream oos = AppendableObjectOutputStream.create(
      new FileOutputStream(new File(FILE_NAME), append), append);
    oos.writeObject(le1);
    oos.writeObject(le2);
    oos.writeObject(le3);
    
    oos.close();
    
    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(FILE_NAME)));
    
    for(int i = 0; i < 100/*ois.available() > 0*/; i++) {
      try {
      LogEntry newle = (LogEntry)ois.readObject();
      System.out.println(newle); 
      } catch(EOFException e) {
        break;
      }
    }
    
    /*
    LogEntry newle1 = (LogEntry)ois.readObject();
    LogEntry newle2 = (LogEntry)ois.readObject();
    LogEntry newle3 = (LogEntry)ois.readObject();
    
    System.out.println(newle1);
    System.out.println(newle2);
    System.out.println(newle3);
    */
   // assertTrue(newle1.equals(le1));
    
    ois.close();
  }

  @Test
  public void testPath() {
    Path path = Paths.get("d:/tmp/aaa");
    Path path2 = path.resolve("raft/test/");
    System.out.println(path2.toFile().getAbsolutePath());
  }
}
