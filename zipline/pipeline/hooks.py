"""Pipeline Engine Hooks
"""
import cgi
import time

from interface import Interface, implements
import logbook
import pandas as pd

from zipline.utils.compat import contextmanager, wraps


class PipelineHooks(Interface):
    """
    Interface for instrumenting PipelineEngine methods.

    Implementations of this interface can be passed to the ``hooks`` parameter
    of SimplePipelineEngine
    """

    def on_create_execution_plan(self, plan):
        """Called on resolution of a Pipeline to an ExecutionPlan.
        """

    @contextmanager
    def running_pipeline(self, pipeline, start_date, end_date, chunked):
        """
        Contextmanager entered during execution of run_pipeline or
        run_chunked_pipeline.
        """

    @contextmanager
    def computing_chunk(self, plan, start_date, end_date):
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
    def running_pipeline(self, pipeline, start_date, end_date, chunked):
        yield

    @contextmanager
    def computing_chunk(self, plan, start_date, end_date):
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
    def running_pipeline(self, pipeline, start_date, end_date, chunked):
        if chunked:
            modifier = " chunked"
        else:
            modifier = ""

        with self._log_duration("running{} pipeline: {} -> {}",
                                modifier, start_date, end_date):
            yield

    @contextmanager
    def computing_chunk(self, plan, start_date, end_date):
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
    def computing_term(self, term):
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
    import ipywidgets
    from IPython.display import display
    HAVE_WIDGETS = True
except ImportError:
    HAVE_WIDGETS = False


class ProgressBarHooks(implements(PipelineHooks)):
    """Hooks that ipywidgets to display progress in a Jupyter Notebook.
    """

    def __init__(self):
        self._bar = None

    @contextmanager
    def running_pipeline(self, pipeline, start_date, end_date, chunked):
        if chunked:
            self._bar = ChunkedProgress(start_date, end_date)
        else:
            self._bar = UnchunkedProgress(start_date, end_date)

        with self._bar:
            try:
                yield
            finally:
                self._bar = None

    @contextmanager
    def computing_chunk(self, plan, start_date, end_date):
        self._bar.start_chunk(len(plan), start_date, end_date)
        try:
            yield
        finally:
            self._bar.finish_chunk()

    @contextmanager
    def loading_terms(self, terms):
        self._bar.start_load_terms(terms)
        try:
            yield
        finally:
            self._bar.finish_load_terms(terms)

    @contextmanager
    def computing_term(self, term):
        self._bar.start_compute_term(term)
        try:
            yield
        finally:
            self._bar.finish_compute_term(term)

    def on_create_execution_plan(self, plan):
        self._bar.set_num_terms(len(plan))


class ChunkedProgress(object):

    def __init__(self, start_date, end_date):
        self._start_date = start_date
        self._end_date = end_date
        self._total_days = (end_date - start_date).days + 1

        self._heading = ipywidgets.HTML()

        self._bar = ipywidgets.IntProgress(
            bar_style='info',
            layout={'width': '600px'},
        )

        self._percent_indicator = ipywidgets.Label()

        self._current_action = ipywidgets.HTML(
            value='<b>Analyzing Pipeline...</b>',
            layout={
                'height': '250px',
            }
        )

        self._layout = ipywidgets.VBox([
            self._heading,
            ipywidgets.HBox(
                [self._percent_indicator, self._bar],
            ),
            self._current_action,
        ])

        self._start_time = None
        self._num_terms = None
        self._days_completed = 0
        self._current_chunk_days = None

    def set_num_terms(self, nterms):
        self._bar.max = nterms * self._total_days

    def __enter__(self):
        self._start_time = time.time()
        display(self._layout)

    def __exit__(self, *args):
        # TODO: Handle error.
        self._bar.bar_style = 'success'
        import humanize
        self._heading.value = "<b>Computed Pipeline:</b> Start={}, End={}".format(self._start_date.date(), self._end_date.date())
        self._current_action.value = "<b>Total Execution Time:</b> {}.".format(humanize.naturaldelta(time.time() - self._start_time))
        self._current_action.layout = {'height': ''}

    def start_chunk(self, nterms, start_date, end_date):
        # +1 to be inclusive of end date.
        days_since_start = (end_date - self._start_date).days + 1
        self._current_chunk_days = days_since_start - self._days_completed
        self._heading.value = (
            "<b>Running Pipeline</b>: Chunk Start={}, Chunk End={}".format(
                start_date.date(), end_date.date(),
            )
        )
        self._increment_progress(1)  # hack: Account for AssetExists().

    def finish_chunk(self):
        self._days_completed += self._current_chunk_days

    def start_load_terms(self, terms):
        header = '<b>Loading Inputs:</b>'
        entries = ''.join([
            '<li><pre>{}</pre></li>'.format(cgi.escape(str(t)))
            for t in terms
        ])
        status = '{}<ul>{}</ul>'.format(header, entries)
        self._current_action.value = status

    def finish_load_terms(self, terms):
        self._increment_progress(nterms=len(terms))

    def start_compute_term(self, term):
        self._current_action.value = '<b>Computing Expression:</b><ul><li><pre>{}</pre></li></ul>'.format(cgi.escape(str(term.recursive_repr())))

    def finish_compute_term(self, term):
        self._increment_progress(nterms=1)

    def _increment_progress(self, nterms):
        self._set_progress(self._bar.value + nterms * self._current_chunk_days)

    def _set_progress(self, term_days):
        bar = self._bar
        bar.value = term_days
        percent = (bar.value - bar.min) / float(bar.max - bar.min) * 100.0
        self._percent_indicator.value = "{0:.2f}% Complete".format(percent)


class UnchunkedProgress(object):

    def __init__(self, start_date, end_date):
        raise AssertionError('not ready')
