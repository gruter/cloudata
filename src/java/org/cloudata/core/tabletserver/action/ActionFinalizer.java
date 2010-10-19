package org.cloudata.core.tabletserver.action;

public abstract class ActionFinalizer {
  boolean alreadyFinalized = false;
  
  public final void doFinalization() {
    if (!alreadyFinalized) {
      finalizing();
      alreadyFinalized = true;
    }
  }
  
  protected abstract void finalizing();
}
