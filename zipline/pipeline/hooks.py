"""Pipeline Engine Hooks
"""
import time

from interface import Interface, implements
import logbook

from zipline.utils.compat import contextmanager, wraps


class PipelineHooks(Interface):

    def on_create_execution_plan(self, plan):
        """Called on resolution of a Pipeline to an ExecutionPlan.
        """

    @contextmanager
    def running_chunked_pipeline(self, start_date, end_date):
        """Contextmanager entered during execution of run_chunked_pipeline.
        """

    @contextmanager
    def computing_chunk(self, start_date, end_date):
        """Contextmanager entered during execution of compute_chunk.
        """

    @contextmanager
    def loading_terms(self, terms):
        """Contextmanager entered when loading a batch of LoadableTerms.
        """

    @contextmanager
    def computing_term(self, term):
        """Contextmanager entered when computing a ComputableTerm.
        """


class NoHooks(implements(PipelineHooks)):
    """A PipelineHooks that defines no-op methods for all available hooks.

    Use this as a base class if you only want to implement a subset of all
    possible hooks.
    """
    def on_create_execution_plan(self, plan):
        pass

    @contextmanager
    def running_chunked_pipeline(self, start_date, end_date):
        yield

    @contextmanager
    def computing_chunk(self, start_date, end_date):
        yield

    @contextmanager
    def loading_terms(self, terms):
        yield

    @contextmanager
    def computing_term(self, term):
        yield


class LoggingHooks(implements(PipelineHooks)):
    """A PipelineHooks that logs information about pipeline progress.
    """
    def __init__(self, logger=None):
        if logger is None:
            logger = logbook.Logger("Pipeline Progress")
        self.log = logger

    def on_create_execution_plan(self, plan):
        # Not worth logging anything here.
        pass

    @contextmanager
    def _log_duration(self, message, *args, **kwargs):
        self.log.info(message, *args, **kwargs)
        start = time.time()
        yield
        end = time.time()
        self.log.info(
            "finished " + message + ". Duration: {duration} seconds.",
            *args, duration=(end - start), **kwargs
        )

    @contextmanager
    def running_chunked_pipeline(self, start_date, end_date):
        with self._log_duration("running pipeline: {} -> {}",
                                start_date, end_date):
            yield

    @contextmanager
    def computing_chunk(self, start_date, end_date):
        with self._log_duration("running pipeline chunk: {} -> {}",
                                start_date, end_date):
            yield

    @contextmanager
    def loading_terms(self, terms):
        # Use the recursive repr b/c it's shorter.
        self.log.info("Loading input batch:")
        for t in terms:
            self.log.info("  - {}", t)
        start_time = time.time()
        yield
        end_time = time.time()
        self.log.info(
            "Finished loading input batch. Duration: {} seconds",
            (end_time - start_time),
        )

    @contextmanager
    def computing_terms(self, term):
        with self._log_duration("computing {}", term):
            yield


def delegating_hooks_method(method_name):
    @wraps(getattr(PipelineHooks, method_name))
    def method(self, *args, **kwargs):
        for hook in self._hooks:
            getattr(hook, method_name)(*args, **kwargs)
    return method


class DelegatingHooks(implements(PipelineHooks)):
    """A PipelineHooks that delegates to one or more other hooks.
    """
    def __new__(cls, hooks):
        if len(hooks) == 0:
            return NoHooks()
        elif len(hooks) == 1:
            return hooks[0]
        else:
            return super(DelegatingHooks, cls).__new__(cls, hooks)

    def __init__(self, hooks):
        self._hooks = hooks

    # Implement all interface methods by delegating to corresponding methods on
    # input hooks.
    locals().update({
        name: delegating_hooks_method(name)
        for name in vars(PipelineHooks)
    })


del delegating_hooks_method


try:
    import tqdm
    HAVE_TQDM = True
except ImportError:
    HAVE_TQDM = False


class ProgressBarHooks(NoHooks):

    def __init__(self):
        if not HAVE_TQDM:
            raise RuntimeError(
                "tqdm must be installed to use ProgressBarHooks"
            )
        self._state = None

    @contextmanager
    def running_chunked_pipeline(self, start_date, end_date):
        try:
            yield
        finally:
            self.__init__()

    def on_create_execution_plan(self, plan):
        pass

    class _State(object):

        def __init__(
