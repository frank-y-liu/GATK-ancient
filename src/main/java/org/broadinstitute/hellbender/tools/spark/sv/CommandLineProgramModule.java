package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import scala.Tuple3;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

/**
 * A class for experimenting with cmd line programs (such as bwa and sga) that's not part of GATK (or has Java bindings yet).
 */
public abstract class CommandLineProgramModule {

    public CommandLineProgramModule(){  }

    /**
     * Builds the initial argument list for the command line (e.g. "bwa mem").
     * @return a list of strings having the names of requested tools
     */
    abstract List<String> initializeCommands(final Path pathToProgram);

    /**
     * Function to execute the command line program, if available, on the provided arguments.
     * @param pathToProgram             full path to the underlying program
     * @param directoryToWorkIn         a directory for the program to work in (mainly for output); can be null
     * @param runtimeArguments          arguments to be provided to the program; can be null (e.g. program like "ls" taking no arguments)
     * @param workingEnvironmentArgs    arguments to setup working environment for the program to run under
     * @return                          exit status of running the process, paired with stdout and stderr strings of the process
     *                                  Several possible values for the returned integer value:
     *                                  0 -- successfully executed the process
     *                                  1 -- program failed to start
     *                                  2 -- program interrupted
     *                                  3 -- failed to capture stdout/stderr from the program
     *                                  10+ -- exit status of underlying program, padded by 10
     *                                  except for case 0, accompanying stdout is null, and stderr contains error message
     */
    public Tuple3<Integer, String, String> run(final Path pathToProgram,
                                               final File directoryToWorkIn,
                                               final List<String> runtimeArguments,
                                               final String... workingEnvironmentArgs) {

        List<String> commands = initializeCommands(pathToProgram);
        if(null!=runtimeArguments && !runtimeArguments.isEmpty()){
            commands.addAll(runtimeArguments);
        }

        final ProcessBuilder builder = new ProcessBuilder(commands);

        setupWorkingEnvironment(builder, workingEnvironmentArgs);
        if(null!=directoryToWorkIn){
            builder.directory(directoryToWorkIn);
        }

        try{
            final Process process = builder.start();
            final InputStream stdoutStream = new BufferedInputStream(process.getInputStream());
            final InputStream stderrStream = new BufferedInputStream(process.getErrorStream());
            String out = "";
            String err = "";
            try{
                out = IOUtils.toString(stdoutStream, Charset.defaultCharset());
                err = IOUtils.toString(stderrStream, Charset.defaultCharset());
                stdoutStream.close();
                stderrStream.close();
            }catch (final IOException ex){
                stdoutStream.close();
                stderrStream.close();
                return new Tuple3<>(3, null, ex.getMessage());
            }
            final int exitStatus = process.waitFor();

            if(0!=exitStatus){
                return new Tuple3<>(10+exitStatus, out, err+String.join(" ", commands) + System.getProperty("line.separator"));
            }

            return new Tuple3<>(0, out, err);
        } catch (final IOException e){
            return new Tuple3<>(1, null, e.getMessage() + Throwables.getStackTraceAsString(e));
        } catch (final InterruptedException e){
            return new Tuple3<>(2, null, e.getMessage() + Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * Sets up working environment for the program.
     * Inheriting classes can, and most likely should, override to do non-trivial work.
     * @param builder process builder for the program
     * @param args    arguments used for setting up the environment
     */
    protected void setupWorkingEnvironment(final ProcessBuilder builder, final String... args){

    }
}
