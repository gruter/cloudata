package org.cloudata.core.commitlog;

import java.io.IOException;

public class CommitLogShutDownException extends IOException {
  private static final long serialVersionUID = 1193528225726985408L;

  public CommitLogShutDownException(IOException e) {
    super(e);
  }
}
