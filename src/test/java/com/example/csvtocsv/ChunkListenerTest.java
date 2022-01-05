package com.example.csvtocsv;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.batch.core.jsr.ChunkListenerAdapter;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.UncheckedTransactionException;
import javax.batch.api.chunk.listener.ChunkListener;
import javax.batch.operations.BatchRuntimeException;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ChunkListenerTest {

    private ChunkListenerAdapter adapter;

    @Mock
    private ChunkListener chunk;
    @Mock
    private ChunkListener delegate;
    @Mock
    private ChunkContext context;

    @Before
    public void setUp() {
         adapter = new ChunkListenerAdapter( delegate);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNullDelegate() {
        adapter = new ChunkListenerAdapter(null);
    }

    @Test
    public void testBeforeChunk() throws Exception {
        adapter.beforeChunk(context);
        verify(delegate).beforeChunk();
    }

    @Test(expected= UncheckedTransactionException.class)
    public void testBeforeChunkException() throws Exception {
        doThrow(new Exception("This is expected")).when(delegate).beforeChunk();
        adapter.beforeChunk(null);
    }

    @Test
    public void testAfterChunk() throws Exception {
        adapter.afterChunk(context);
        verify(delegate).afterChunk();
    }

    @Test(expected=UncheckedTransactionException.class)
    public void testAfterChunkException() throws Exception {
        doThrow(new Exception("This is expected")).when(delegate).afterChunk();
        adapter.afterChunk(null);
    }

    @Test(expected= BatchRuntimeException.class)
    public void testAfterChunkErrorNullContext() throws Exception {
        adapter.afterChunkError(null);
    }

    @Test(expected=UncheckedTransactionException.class)
    public void testAfterChunkErrorException() throws Exception {
        doThrow(new Exception("This is expected")).when(delegate).afterChunk();
        adapter.afterChunk(null);
    }





}
