package jnr.ffi;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;

import java.util.HashMap;
import java.util.Map;



public class Example {

    public interface LibC {
        int setenv(String name, String value, boolean overwrite); // overwrite can be int but boolean makes more sense

        int unsetenv(String name);

        String getenv(String name);

        int clearenv();

//        long add_c(long a, long b);

        Pointer new_lakesoul_reader_config_builder();

        Pointer lakesoul_config_builder_add_file(Pointer builder,Pointer files ,int file_num);

    }

    class Timespec extends Struct {

        protected Timespec(Runtime runtime) {
            super(runtime);
        }
    }


        public static void main(String[] args) {
        Map<LibraryOption, Object> libraryOptions = new HashMap<>();
        libraryOptions.put(LibraryOption.LoadNow, true);
        libraryOptions.put(LibraryOption.IgnoreError, true);
        String libName = "/Users/ceng/Documents/GitHub/LakeSoul/native-io/target/debug/liblakesoul_io_c.dylib"; // platform specific name for libC

        LibC libc = LibraryLoader.loadLibrary(
                LibC.class,
                libraryOptions,
                libName
        );

        final String pwdKey = "PWD"; // key for working directory
        final String shellKey = "SHELL"; // key for system shell (bash, zsh etc)

        String pwd = libc.getenv(pwdKey);
//        System.out.println(libc.add_c(2,3));
        Pointer builder = libc.new_lakesoul_reader_config_builder();
        System.out.println(libc.lakesoul_config_builder_add_file(builder, builder, 0));

        System.out.println(pwd); // prints current directory

        libc.setenv(pwdKey, "/", true); // set PWD to /
        System.out.println(libc.getenv(pwdKey)); // prints /

        libc.unsetenv(pwdKey); // unset PWD
        System.out.println(libc.getenv(pwdKey)); // prints null (it is null not the String "null")

        System.out.println(libc.getenv(shellKey)); // prints system shell, /bin/bash on most Unixes
//        libc.clearenv(); // clear all environment variables
        System.out.println(libc.getenv(shellKey)); // prints null (it is null not the String "null")
        System.out.println(libc.getenv("_")); // even the special "_" environment variable is now null


    }
}