package com.github.dtf.rpc;

import java.lang.reflect.Constructor;  
import java.lang.reflect.InvocationTargetException;  
import java.lang.reflect.Method;  
  
import com.github.dtf.rpc.protocol.Calculator;  
  
import com.google.protobuf.BlockingService;  
  
public class CalculatorService implements Calculator {      
      
    private Server server = null;  
    private final Class protocol = Calculator.class;  
    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();  
    private final String protoPackage = "org.tao.pbtest.proto";  
    private final String host = "localhost";  
    private final int port = 8038;  
      
    public CalculatorService (){  
          
    }  
      
    @Override  
    public int add(int a, int b) {  
        // TODO Auto-generated method stub  
        return a+b;  
    }  
  
    public int minus(int a, int b){  
        return a-b;  
    }  
      
      
    public void init(){  
        createServer();          
    }  
      
      
    /* 
     * return org.tao.pbtest.server.api.CalculatorPBServiceImpl 
     */  
    public Class<?> getPbServiceImplClass(){  
        String packageName = protocol.getPackage().getName();  
        String className = protocol.getSimpleName();  
        String pbServiceImplName =  packageName + "." + className +  "PBServiceImpl";          
        Class<?> clazz = null;  
        try{  
            clazz = Class.forName(pbServiceImplName, true, classLoader);  
        }catch(ClassNotFoundException e){  
            System.err.println(e.toString());  
        }  
        return clazz;  
    }  
      
    /* 
     * return org.tao.pbtest.proto.Calculator$CalculatorService 
     */  
    public Class<?> getProtoClass(){  
        String className = protocol.getSimpleName();  
        String protoClazzName =  protoPackage + "." + className + "$" + className + "Service";  
        Class<?> clazz = null;  
        try{  
            clazz = Class.forName(protoClazzName, true, classLoader);  
        }catch(ClassNotFoundException e){  
            System.err.println(e.toString());  
        }  
        return clazz;  
    }  
      
    public void createServer(){  
        Class<?> pbServiceImpl = getPbServiceImplClass();  
        Constructor<?> constructor = null;  
        try{  
            constructor = pbServiceImpl.getConstructor(protocol);  
            constructor.setAccessible(true);  
        }catch(NoSuchMethodException e){  
            System.err.print(e.toString());  
        }  
          
        Object service = null;  // instance of CalculatorPBServiceImpl  
        try {  
            service = constructor.newInstance(this);  
        }catch(InstantiationException e){  
        } catch (IllegalArgumentException e) {  
        } catch (IllegalAccessException e) {  
        } catch (InvocationTargetException e) {  
        }  
          
        /* 
         * interface: org.tao.pbtest.server.CalculatorPB 
         */  
        Class<?> pbProtocol = service.getClass().getInterfaces()[0];  
                  
        /* 
         * class: org.tao.pbtest.proto.Calculator$CalculatorService 
         */  
        Class<?> protoClazz = getProtoClass();  
          
        Method method = null;  
        try {  
  
            // pbProtocol.getInterfaces()[] ¼´ÊÇ½Ó¿Ú org.tao.pbtest.proto.Calculator$CalculatorService$BlockingInterface  
  
            method = protoClazz.getMethod("newReflectiveBlockingService", pbProtocol.getInterfaces()[0]);  
            method.setAccessible(true);  
        }catch(NoSuchMethodException e){  
            System.err.print(e.toString());  
        }  
          
        try{  
            createServer(pbProtocol, (BlockingService)method.invoke(null, service));  
        }catch(InvocationTargetException e){  
        } catch (IllegalArgumentException e) {  
        } catch (IllegalAccessException e) {  
        }  
          
    }  
      
    public void createServer(Class pbProtocol, BlockingService service){  
        server = new Server(pbProtocol, service, port);  
        server.start();  
    }  
      
    public static void main(String[] args){  
        CalculatorService cs = new CalculatorService();  
        cs.init();  
    }  
}  