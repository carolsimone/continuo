import { useNavigate } from 'react-router';

interface Props {
  // Dormant nodes of the live release: nodes continuo builds that have no
  // schedule tag. A run started by hand executes them, and any run whose load
  // of a seed drops the seed's table only rebuilds a dormant view or
  // materialized view below that seed.
  count: number;
}

// The Topology tab's tile for the dormant nodes, after the schedule tiles: it
// opens the Nodes tab filtered to them.
export default function DormantTile({ count }: Props) {
  const navigate = useNavigate();
  return (
    <button type="button" className="snapshot-tile" onClick={() => navigate('/?tab=nodes&dormant=1')}>
      <div className="snapshot-tile__name">Dormant</div>
      <div className="snapshot-tile__meta">
        {count} dormant {count === 1 ? 'node' : 'nodes'} · run by hand
      </div>
    </button>
  );
}
