import { ReleaseTransition } from './types';
import { PipelineRun, releasePillClass, upcomingStages } from './release-helpers';

// PipelineTimeline shows a run's path through the pipeline: one step per
// recorded transition, coloured like the run's status pill and stamped with
// when it happened, followed — while the run is still in flight — by the
// stages its kind can still pass through (see upcomingStages): ghosted,
// undated, read out as "(upcoming)" so they are not mistaken for completed
// steps by ear, and — for a stage that does not run on every path — captioned
// with what it depends on.
export default function PipelineTimeline({ transitions, run = {} }: {
  transitions: ReleaseTransition[];
  run?: PipelineRun;
}) {
  return (
    <>
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Timeline</span>
        </div>
      </div>
      <ol className="release-timeline">
        {transitions.map((t, i) => (
          <li key={`${i}-${t.to}`} className="release-timeline__step">
            <span className="release-timeline__connector" aria-hidden="true" />
            <div className="release-timeline__body">
              <span className={`pill-sm ${releasePillClass(t.to).replace('pill--', 'pill-sm--')}`}>{t.to}</span>
              <span className="release-timeline__at">{t.at.slice(0, 19).replace('T', ' ')}</span>
            </div>
          </li>
        ))}
        {upcomingStages(transitions, run).map(({ stage, condition }) => (
          <li key={stage} className="release-timeline__step release-timeline__step--upcoming">
            <span className="release-timeline__connector" aria-hidden="true" />
            <div className="release-timeline__body">
              <span className="pill-sm">{stage}<span className="sr-only"> (upcoming)</span></span>
              {condition && <span className="release-timeline__hint">{condition}</span>}
            </div>
          </li>
        ))}
      </ol>
    </>
  );
}
