package com.chicm.cmraft.log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class AppendableObjectOutputStream extends ObjectOutputStream {

  public AppendableObjectOutputStream(OutputStream out) throws IOException {
    super(out);
  }
  
  public static ObjectOutputStream create(OutputStream out, boolean append) throws IOException {
    if(append) {
      return new AppendableObjectOutputStream(out);
    } else {
      return new ObjectOutputStream(out);
    }
  }

  @Override
  protected void writeStreamHeader() throws IOException{
  // do not write a header
  }
}
