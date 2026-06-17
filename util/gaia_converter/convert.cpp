// Photometric conversions: G→V and BP-RP→B-V
// Formulas match Cartes du Ciel (cu_catalog.pas:6500-6530)

#include "convert.hpp"
#include <cmath>
#include <algorithm>

double g_to_v(double g, double c)
{
	// Official Gaia DR3 G→V relation:
	//   V = G + 0.02704 − 0.01424·C + 0.2156·C² − 0.01426·C³
	// where C = G_BP − G_RP
	return g + 0.02704 - 0.01424*c + 0.2156*c*c - 0.01426*c*c*c;
}

double bp_rp_to_bv(double c)
{
	// Step 1: BP-RP → Tycho2 BT-VT (Gaia EDR3 5th order polynomial)
	double bt_vt = -0.006482 + 0.7865*c - 0.3631*c*c + 0.93192*c*c*c - 0.4843*c*c*c*c + 0.06814*c*c*c*c*c;

	// Step 2: BT-VT → Johnson B-V (Mamajek 2006 piecewise)
	double bv;
	if (bt_vt < 0.5) {
		// -0.25 < BT-VT < 0.5
		bv = bt_vt - 0.006 - 0.1069*bt_vt + 0.1459*bt_vt*bt_vt;
	} else if (bt_vt < 2.0) {
		// 0.5 ≤ BT-VT < 2.0
		bv = bt_vt - 0.007813*bt_vt - 0.1489*bt_vt*bt_vt + 0.03384*bt_vt*bt_vt*bt_vt;
	} else {
		// BT-VT ≥ 2.0
		bv = 0.850 * bt_vt;
	}
	return std::max(0.0, std::min(bv, 5.35));
}
