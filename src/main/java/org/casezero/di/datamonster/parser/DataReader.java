package org.casezero.di.datamonster.parser;

import java.io.IOException;
import java.util.List;

import org.casezero.di.datamonster.Field;

public interface DataReader {
    public List<String> getNext() throws IOException;
    public List<Field> getHeaders();
}
