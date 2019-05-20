"""Pipeline hooks for tracking and displaying progress.
"""
import cgi
import time

from interface import implements, Interface

from zipline.utils.compat import contextmanager

from .iface import PipelineHooks


try:
    import ipywidgets
    from IPython.display import display
    HAVE_WIDGETS = True
except ImportError:
    HAVE_WIDGETS = False


class ProgressHooks(implements(PipelineHooks)):
    """
    Hooks implementation for displaying progress.

    Parameters
    ----------
    display_factory : callable
        Function from ``(start_date, end_date)`` to an object implementing
        ``ProgressDisplay``.
    """
    def __init__(self, display_factory):
        self._display_factory = display_factory
        self._display = None

    @classmethod
    def with_widgets(cls):
        """Construct a ProgressHooks that displays to a Jupyter widget.
        """
        return cls(display_factory=WidgetProgressDisplay)

    @contextmanager
    def running_pipeline(self, pipeline, start_date, end_date):
        self._display = self._display_factory(start_date, end_date)
        try:
            self._display.start_run_pipeline(pipeline, start_date, end_date)
            yield
        except Exception:
            self._display.finish_run_pipeline(
                pipeline,
                start_date,
                end_date,
                success=False,
            )
            raise
        else:
            self._display.finish_run_pipeline(
                pipeline,
                start_date, end_date,
                success=True,
            )
        finally:
            self._display = None

    def on_create_execution_plan(self, plan):
        self._display.set_execution_plan(plan)

    @contextmanager
    def computing_chunk(self, plan, start_date, end_date):
        try:
            self._display.start_chunk(start_date, end_date)
            yield
        finally:
            self._display.finish_chunk(start_date, end_date)

    @contextmanager
    def loading_terms(self, terms):
        try:
            self._display.start_load_terms(terms)
            yield
        finally:
            self._display.finish_load_terms(terms)

    @contextmanager
    def computing_term(self, term):
        try:
            self._display.start_compute_term(term)
            yield
        finally:
            self._display.finish_compute_term(term)


class ProgressDisplay(Interface):
    """Interface for types that can display progress for a ProgressHooks.
    """
    # NOTE: Methods of this interface are laid out in by expected call order of
    # "start_*" methods.
    def start_run_pipeline(self, pipeline, start_date, end_date):
        pass

    def finish_run_pipeline(self, pipeline, start_date, end_date, success):
        pass

    def set_execution_plan(self, plan):
        pass

    def start_chunk(self, start_date, end_date):
        pass

    def finish_chunk(self, start_date, end_date):
        pass

    def start_load_terms(self, terms):
        pass

    def finish_load_terms(self, terms):
        pass

    def start_compute_term(self, term):
        pass

    def finish_compute_term(self, term):
        pass


class WidgetProgressDisplay(implements(ProgressDisplay)):
    """A progress display that renders to an IPython widget.
    """

    def __init__(self, start_date, end_date):
        self._start_date = start_date
        self._end_date = end_date
        self._total_days = (end_date - start_date).days + 1

        # Heading for progress display.
        self._heading = ipywidgets.HTML()

        # Percent Complete Indicator to the left of the bar.
        indicator_width = '120px'
        self._percent_indicator = ipywidgets.HTML(
            layout={'width': indicator_width},
        )

        # The progress bar itself.
        #
        # NOTE: This isn't fully initialized yet, because we don't what value
        # to use for ``max`` until `set_execution_plan` is called. We complete
        # initialization of this in ``set_execution_plan``.
        self._bar = ipywidgets.IntProgress(
            bar_style='info',
            # Leave enough space for the percent indicator.
            layout={'width': 'calc(100% - {})'.format(indicator_width)},
        )
        bar_and_percent = ipywidgets.HBox([self._percent_indicator, self._bar])

        # Collapsable details tab underneath the progress bar.
        self._details_body = ipywidgets.HTML(
            value='<b>Analyzing Pipeline...</b>',
        )
        self._details_tab = ipywidgets.Accordion(
            children=[self._details_body],
            selected_index=None,  # Start in collapsed state.
            layout={
                # Override default border settings to make details tab less
                # heavyweight.
                'border': '1px',
            },
        )
        # There's no public interface for setting title in the constructor :/.
        self._details_tab.set_title(0, 'Details')

        # Container for the combined widget.
        self._layout = ipywidgets.VBox(
            [
                self._heading,
                bar_and_percent,
                self._details_tab,
            ],
            # Overall layout consumes 75% of the page.
            layout={'width': '75%'},
        )

        # Used to display total execution time at the end of the run.
        self._start_time = None

        # Number of terms in the execution plan of the pipeline being run.
        self._num_terms = None

        # Total number of days completed so far.
        self._days_completed = 0

        # Number of days in the current chunk.
        self._current_chunk_days = None

        self._displayed = False

    def set_execution_plan(self, plan):
        # set_execution_plan will be called once for each chunk. We only need
        # to update our state the first time through.
        if self._displayed:
            return

        # HACK: Subtract 1 to account for AssetExists.
        #
        # TODO: This won't work if someone uses InputDates() (or, I think, a
        # term whose value is stored in initial_workspace). Is there a better
        # way to account for terms that don't get computed?
        self._num_terms = len(plan) - 1
        self._bar.min = 0
        self._bar.max = self._num_terms * self._total_days

        self._set_progress(0)

        display(self._layout)

        self._displayed = True

    def start_run_pipeline(self, pipeline, start_date, end_date):
        self._start_time = time.time()

    def finish_run_pipeline(self, pipeline, start_date, end_date, success):
        execution_time = time.time() - self._start_time
        if not success:
            self._bar.bar_style = 'danger'
            return

        self._bar.bar_style = 'success'
        self._heading.value = (
            "<b>Computed Pipeline:</b> Start={}, End={}"
            .format(self._start_date.date(), self._end_date.date())
        )
        self._details_body.value = (
            "<b>Total Execution Time:</b> {}".format(
                self._format_execution_time(execution_time)
            )
        )
        # Shrink display area back down to only require the amount needed.
        self._details_body.layout = {'height': ''}

    def start_chunk(self, start_date, end_date):
        # Compute the "length" of this chunk as the number of days between the
        # end_date and the overall start_date, minus the
        days_since_start = (end_date - self._start_date).days + 1

        self._current_chunk_days = days_since_start - self._days_completed
        self._heading.value = (
            "<b>Running Pipeline</b>: Chunk Start={}, Chunk End={}".format(
                start_date.date(), end_date.date(),
            )
        )

    def finish_chunk(self, start_date, end_date):
        self._days_completed += self._current_chunk_days

    def start_load_terms(self, terms):
        header = '<b>Loading Inputs:</b>'
        # Render terms as a bulleted list of monospace strings.
        entries = ''.join([
            '<li><pre>{}</pre></li>'.format(cgi.escape(str(t)))
            for t in terms
        ])
        status = '{}<ul>{}</ul>'.format(header, entries)

        self._details_body.value = status

    def finish_load_terms(self, terms):
        self._increment_progress(nterms=len(terms))

    def start_compute_term(self, term):
        header = '<b>Computing Expression:</b>'
        # Render terms as a length-1 bulleted list of monospace strings.
        # We render as a list to keep the formatting consistent between loading
        # and computing.
        entries = '<li><pre>{}</pre></li>'.format(
            cgi.escape(str(term.recursive_repr()))
        )
        status = '{}<ul>{}</ul>'.format(header, entries)

        self._details_body.value = status

    def finish_compute_term(self, term):
        self._increment_progress(nterms=1)

    def _increment_progress(self, nterms):
        self._set_progress(self._bar.value + nterms * self._current_chunk_days)

    def _set_progress(self, term_days):
        bar = self._bar
        pct = (term_days - bar.min) / float(bar.max - bar.min) * 100.0

        self._bar.value = term_days
        self._percent_indicator.value = "<b>{:.2f}% Complete</b>".format(pct)

    @staticmethod
    def _format_execution_time(total_seconds):
        """Helper method for displaying total execution time of a Pipeline.

        Parameters
        ----------
        total_seconds : float
            Number of seconds elapsed.

        Returns
        -------
        formatted : str
            User-facing text representation of elapsed time.
        """
        def maybe_s(n):
            if n == 1:
                return ''
            return 's'

        minutes, seconds = divmod(total_seconds, 60)
        minutes = int(minutes)
        if minutes >= 60:
            hours, minutes = divmod(minutes, 60)
            t = "{hours} Hour{hs}, {minutes} Minute{ms}, {seconds:.2f} Seconds"
            return t.format(
                hours=hours, hs=maybe_s(hours),
                minutes=minutes, ms=maybe_s(minutes),
                seconds=seconds,
            )
        elif minutes >= 1:
            t = "{minutes} Minute{ms}, {seconds:.2f} Seconds"
            return t.format(
                minutes=minutes,
                ms=maybe_s(minutes),
                seconds=seconds,
            )
        else:
            return "{seconds:.2f} Seconds".format(seconds=seconds)


# class LoggingHooks(implements(PipelineHooks)):
#     """A PipelineHooks that logs information about pipeline progress.
#     """
#     def __init__(self, logger=None):
#         if logger is None:
#             logger = logbook.Logger("Pipeline Progress")
#         self.log = logger

#     def on_create_execution_plan(self, plan):
#         pass

#     @contextmanager
#     def _log_duration(self, message, *args, **kwargs):
#         self.log.info(message, *args, **kwargs)
#         start = time.time()
#         yield
#         end = time.time()
#         self.log.info(
#             "finished " + message + ". Duration: {duration} seconds.",
#             *args, duration=(end - start), **kwargs
#         )

#     @contextmanager
#     def running_pipeline(self, pipeline, start_date, end_date, chunked):
#         if chunked:
#             modifier = " chunked"
#         else:
#             modifier = ""

#         with self._log_duration("running{} pipeline: {} -> {}",
#                                 modifier, start_date, end_date):
#             yield

#     @contextmanager
#     def computing_chunk(self, plan, start_date, end_date):
#         with self._log_duration("running pipeline chunk: {} -> {}",
#                                 start_date, end_date):
#             yield

#     @contextmanager
#     def loading_terms(self, terms):
#         # Use the recursive repr b/c it's shorter.
#         self.log.info("Loading input batch:")
#         for t in terms:
#             self.log.info("  - {}", t)
#         start_time = time.time()
#         yield
#         end_time = time.time()
#         self.log.info(
#             "Finished loading input batch. Duration: {} seconds",
#             (end_time - start_time),
#         )

#     @contextmanager
#     def computing_term(self, term):
#         with self._log_duration("computing {}", term):
#             yield
