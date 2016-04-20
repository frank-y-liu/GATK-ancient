package org.broadinstitute.hellbender.tools.spark.sv;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an SGA module that can be called via "run" (in base) to do actual work.
 */
public final class SGAModule extends CommandLineProgramModule {

    private final String moduleName;

    public SGAModule(final String moduleName){
        this.moduleName = moduleName;
    }

    @Override
    public List<String> initializeCommands(final Path pathToSGA) {
        final ArrayList<String> res = new ArrayList<>();
        res.add(pathToSGA.toString());
        res.add(moduleName);
        return res;
    }
}
