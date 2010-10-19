package org.cloudata.core.common.util;

import java.util.Random;

public class NumberUtil {
  public static double randomWithRange(double ratio) {
    return 1.0 - ratio + (2*ratio) * new Random().nextDouble();
  }
}
