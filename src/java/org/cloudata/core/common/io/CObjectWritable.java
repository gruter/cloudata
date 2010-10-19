/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cloudata.core.common.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CConfigurable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.conf.CConfigured;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;


/** A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
public class CObjectWritable implements CWritable, CConfigurable {
  public static final Log LOG = LogFactory.getLog(CObjectWritable.class.getName());
  
  public static final short TYPE_SAME = 0;
  public static final short TYPE_DIFF = 1;
  
  private Class declaredClass;
  private Object instance;
  private CloudataConf conf;

  public CObjectWritable() {}
  
  public CObjectWritable(Object instance) {
    set(instance);
  }

  public CObjectWritable(Class declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public Object get() { return instance; }
  
  /** Return the class this is meant to be. */
  public Class getDeclaredClass() { return declaredClass; }
  
  /** Reset the instance. */
  public void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }
  
  public String toString() {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }

  
  public void readFields(DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }
  
  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }

  private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();
  static {
    PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
    PRIMITIVE_NAMES.put("byte", Byte.TYPE);
    PRIMITIVE_NAMES.put("char", Character.TYPE);
    PRIMITIVE_NAMES.put("short", Short.TYPE);
    PRIMITIVE_NAMES.put("int", Integer.TYPE);
    PRIMITIVE_NAMES.put("long", Long.TYPE);
    PRIMITIVE_NAMES.put("float", Float.TYPE);
    PRIMITIVE_NAMES.put("double", Double.TYPE);
    PRIMITIVE_NAMES.put("void", Void.TYPE);
  }

  private static class NullInstance extends CConfigured implements CWritable {
    private Class<?> declaredClass;
    public NullInstance() { super(null); }
    public NullInstance(Class declaredClass, CloudataConf conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }
    public void readFields(DataInput in) throws IOException {
      String className = CUTF8.readString(in);
      declaredClass = PRIMITIVE_NAMES.get(className);
      if (declaredClass == null) {
        try {
          declaredClass = getConf().getClassByName(className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e.toString());
        }
      }
    }
    public void write(DataOutput out) throws IOException {
      CUTF8.writeString(out, declaredClass.getName());
    }
  }
  
  public static void writeObject(DataOutput out, Object instance,
      Class declaredClass, 
      CloudataConf conf) throws IOException {
    writeObject(out, instance, declaredClass, conf, false);
  }

  /** Write a {@link CWritable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static void writeObject(DataOutput out, Object instance,
                                 Class declaredClass, 
                                 CloudataConf conf, boolean arrayComponent) throws IOException {

    if (instance == null) {                       // null
      instance = new NullInstance(declaredClass, conf);
      declaredClass = CWritable.class;
      arrayComponent = false;
    }

    if(!arrayComponent) {
      CUTF8.writeString(out, declaredClass.getName()); // always write declared
      //System.out.println("Write:declaredClass.getName():" + declaredClass.getName());
    }

    if (declaredClass.isArray()) {                // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      //System.out.println("Write:length:" + length);

      if(declaredClass.getComponentType() == Byte.TYPE) {
        out.write((byte[])instance);
      } else if(declaredClass.getComponentType() == ColumnValue.class) {
        //ColumnValue인 경우 Deserialize하는데 속도를 줄이기 위해 전체 데이터 사이즈를 미리 전송한다.
        writeColumnValue(out, instance, declaredClass, conf, length);
      } else {
        for (int i = 0; i < length; i++) {
          writeObject(out, Array.get(instance, i),
                      declaredClass.getComponentType(), conf, !declaredClass.getComponentType().isArray());
        }
      }
    } else if (declaredClass == String.class) {   // String
      CUTF8.writeString(out, (String)instance);
      
    } else if (declaredClass.isPrimitive()) {     // primitive type

      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isEnum()) {         // enum
      CUTF8.writeString(out, ((Enum)instance).name());
    } else if (CWritable.class.isAssignableFrom(declaredClass)) { // Writable
      if(instance.getClass() == declaredClass) {
        out.writeShort(TYPE_SAME);    //선언한 클래스와 인스턴스의 클래스가 동일한 경우
        //System.out.println("Write:TYPE_SAME:" + TYPE_SAME);

      } else {
        out.writeShort(TYPE_DIFF);
        //System.out.println("Write:TYPE_DIFF:" + TYPE_DIFF);
        CUTF8.writeString(out, instance.getClass().getName());
        //System.out.println("Write:instance.getClass().getName():" + instance.getClass().getName());
      }
      ((CWritable)instance).write(out);
      //System.out.println("Write:instance value");

    } else {
      throw new IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }

  private static void writeColumnValue(DataOutput out, Object instance, Class declaredClass, 
                                            CloudataConf conf, int length) throws IOException {
    int shortByteSize = CWritableUtils.getShortByteSize();
    int intByteSize = CWritableUtils.getIntByteSize();
    
    int totalByteSize = 0;
    
    long startTime = System.currentTimeMillis();
    for(int i = 0; i < length; i++) {
      ColumnValue columnValue = (ColumnValue)Array.get(instance, i);
      totalByteSize += shortByteSize;
      totalByteSize += columnValue.size();
    }
//    long endTime = System.currentTimeMillis();
    //LOG.fatal("writeColumnValue1:length=" + length + ",bytes=" + totalByteSize + ",time=" + (endTime - startTime));
    
    out.writeInt(totalByteSize);
    
//    startTime = System.currentTimeMillis();
    for (int i = 0; i < length; i++) {
      writeObject(out, Array.get(instance, i),
                  declaredClass.getComponentType(), conf, true);
    }
    long endTime = System.currentTimeMillis();
    //LOG.fatal("writeColumnValue2:time=" + (endTime - startTime));
  }
  
  private static Object readColumnValue(DataInput in, CloudataConf conf, Class<?> declaredClass, int length) throws IOException {
    long startTime = System.currentTimeMillis();
    
    int totalByteSize = in.readInt();
    byte[] buf = new byte[totalByteSize];
    in.readFully(buf);
    long endTime = System.currentTimeMillis();
    //LOG.fatal("readColumnValue1:length=" + length + ",bytes=" + totalByteSize + ",time=" + (endTime - startTime));
    
    DataInputStream byteDataIn = new DataInputStream(new ByteArrayInputStream(buf));
    
    ColumnValue[] instance = (ColumnValue[])Array.newInstance(declaredClass.getComponentType(), length);
    //startTime = System.currentTimeMillis();
    Class componentClass = declaredClass.getComponentType();
    for (int i = 0; i < length; i++) {
      //Array.set(instance, i, readObject(byteDataIn, null, conf, true, declaredClass.getComponentType()));
      instance[i] = (ColumnValue)readObject(byteDataIn, null, conf, true, componentClass);
    }
    byteDataIn.close();
    byteDataIn = null;
    buf = null;
    //endTime = System.currentTimeMillis();
    //LOG.fatal("readColumnValue2:time=" + (endTime - startTime));
    return instance;
  }

  
  
  /** Read a {@link CWritable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static Object readObject(DataInput in, CloudataConf conf)
    throws IOException {
    return readObject(in, null, conf, false, null);
  }
    
  public static Object readObject(DataInput in, CObjectWritable objectWritable, 
      CloudataConf conf) throws IOException {
    return readObject(in, objectWritable, conf, false, null);
  }
  
  /** Read a {@link CWritable}, {@link String}, primitive type, or an array of
   * the preceding. */
  @SuppressWarnings("unchecked")
  public static Object readObject(DataInput in, CObjectWritable objectWritable, 
                                      CloudataConf conf, 
                                      boolean arrayComponent,
                                      Class componentClass) throws IOException {
    String className; 
    if(arrayComponent) {
      className = componentClass.getName();
    } else {
      className = CUTF8.readString(in);
		  //SANGCHUL
   //   System.out.println("SANGCHUL] className:" + className);
    }

    Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
    if (declaredClass == null) {
      try {
        declaredClass = conf.getClassByName(className);
      } catch (ClassNotFoundException e) {
		  //SANGCHUL
		  e.printStackTrace();
        throw new RuntimeException("readObject can't find class[className=" + className + "]", e);
      }
    }    

    Object instance;
    
    if (declaredClass.isPrimitive()) {            // primitive types

      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }

    } else if (declaredClass.isArray()) {              // array
      //System.out.println("SANGCHUL] is array");
      int length = in.readInt();
      //System.out.println("SANGCHUL] array length : " + length);
      //System.out.println("Read:in.readInt():" + length);
      if(declaredClass.getComponentType() == Byte.TYPE) {
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        instance = bytes;
      } else if(declaredClass.getComponentType() == ColumnValue.class) {
        instance = readColumnValue(in, conf, declaredClass, length);
      } else {
        Class componentType = declaredClass.getComponentType();
		
			// SANGCHUL
		//System.out.println("SANGCHUL] componentType : " + componentType.getName());

        instance = Array.newInstance(componentType, length);
        for (int i = 0; i < length; i++) {
          Object arrayComponentInstance = readObject(in, null, conf, !componentType.isArray(), componentType);
          Array.set(instance, i, arrayComponentInstance);
          //Array.set(instance, i, readObject(in, conf));
        }
      }
    } else if (declaredClass == String.class) {        // String
      instance = CUTF8.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((Class<? extends Enum>) declaredClass, CUTF8.readString(in));
    } else if (declaredClass == ColumnValue.class) {
      //ColumnValue인 경우 데이터전송 속도를 높이기 위해 별도로 정의하였다.
      //객체가 많은 경우 리플렉션으로 객체를 생성하는 비용이 많이 든다. 
      Class instanceClass = null;
      try {
        short typeDiff = in.readShort();
        if(typeDiff == TYPE_DIFF) {
          instanceClass = conf.getClassByName(CUTF8.readString(in));
        } else {
          instanceClass = declaredClass;
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("readObject can't find class", e);
      }
      ColumnValue columnValue = new ColumnValue();
      columnValue.readFields(in);
      instance = columnValue;
    } else {                                      // Writable

      Class instanceClass = null;
      try {
        short typeDiff = in.readShort();
			// SANGCHUL
	//System.out.println("SANGCHUL] typeDiff : " + typeDiff);
        //System.out.println("Read:in.readShort():" + typeDiff);
        if(typeDiff == TYPE_DIFF) {
			// SANGCHUL
String classNameTemp = CUTF8.readString(in);
	//System.out.println("SANGCHUL] typeDiff : " + classNameTemp);
          instanceClass = conf.getClassByName(classNameTemp);
          //System.out.println("Read:UTF8.readString(in):" + instanceClass.getClass());
        } else {
          instanceClass = declaredClass;
        }
      } catch (ClassNotFoundException e) {

		// SANGCHUL
		  e.printStackTrace();
        throw new RuntimeException("readObject can't find class", e);
      }
      
      CWritable writable = CWritableFactories.newInstance(instanceClass, conf);
      writable.readFields(in);
      //System.out.println("Read:writable.readFields(in)");
      instance = writable;

      if (instanceClass == NullInstance.class) {  // null
        declaredClass = ((NullInstance)instance).declaredClass;
        instance = null;
      }
    }

    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }

    return instance;
  }

  public void setConf(CloudataConf conf) {
    this.conf = conf;
  }

  public CloudataConf getConf() {
    return this.conf;
  }
}
