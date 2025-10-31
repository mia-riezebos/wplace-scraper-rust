const DIVISIONS: f64 = 2048.0;
const SUBDIVISIONS: f64 = 1000.0;
const MAX_MERCATOR: f64 = 20037508.34;
const RADIUS: f64 = 6378137.0;

pub struct LatLon {
    pub lon: f64,
    pub lat: f64,
}

pub fn grid_to_lat_lon(x_grid: f64, y_grid: f64, centered_at_cell: bool) -> LatLon {
    let mut adjusted_x = x_grid;
    let mut adjusted_y = y_grid;

    if centered_at_cell {
        adjusted_x += 0.5 / SUBDIVISIONS;
        adjusted_y += 0.5 / SUBDIVISIONS;
    }

    let x_mercator = (adjusted_x / DIVISIONS) * (2.0 * MAX_MERCATOR) - MAX_MERCATOR;
    let y_mercator = MAX_MERCATOR - (adjusted_y / DIVISIONS) * (2.0 * MAX_MERCATOR);

    let lon = (x_mercator / RADIUS) * (180.0 / std::f64::consts::PI);
    let lat = (2.0 * (y_mercator / RADIUS).exp().atan() - std::f64::consts::PI / 2.0)
        * (180.0 / std::f64::consts::PI);

    LatLon { lon, lat }
}

pub fn generate_url(x_grid: f64, y_grid: f64, zoom_level: &str, centered_at_cell: bool) -> String {
    let coords = grid_to_lat_lon(x_grid, y_grid, centered_at_cell);
    format!("https://wplace.live/?lng={}&lat={}&zoom={}", coords.lon, coords.lat, zoom_level)
}

