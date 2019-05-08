"""Pipeline Engine Hooks
"""
import time

from interface import Interface, implements

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
    def computing_term(self, term):
        """Contextmanager entered during computation of a term.
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
    def computing_term(self, term):
        yield


class LogProgressHook(implements(PipelineHooks)):
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
    def running_chunked_pipeline(self, start_date, end_date):
        self.log.info("Running pipeline: {} -> {}", start_date, end_date)
        start_time = time.time()
        yield
        end_time = time.time()
        self.log.info(
            "Finished running pipeline: {} -> {}. Execution Time: {} seconds.",
            start_date, end_date, (end_time - start_time),
        )

    @contextmanager
    def computing_chunk(self, start_date, end_date):
        self.log.info("Running pipeline chunk: {} -> {}", start_date, end_date)
        start_time = time.time()
        yield
        end_time = time.time()
        self.log.info(
            "Finished running pipeline chunk: {} -> {}. "
            "Execution Time: {} seconds.",
            start_date, end_date, (end_time - start_time),
        )

    @contextmanager
    def computing_term(self, term):
        repr_ = term.short_repr()
        self.log.info("Computing {}", repr_)
        start_time = time.time()
        yield
        end_time = time.time()
        self.log.info(
            "Finished computing {}. Execution Time: {} seconds",
            repr_, (end_time - start_time),
        )

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
