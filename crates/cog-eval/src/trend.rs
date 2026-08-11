//! 进化趋势可视化。
//! - TrendReport/TrendPoint：多轮进化指标序列；
//! - ConvergenceAnalysis：平台期检测（收敛轮数 + 收敛速率）；
//! - render_trend_chart：多指标 SVG 折线图；
//! - render_ablation_stacked_bar：消融边际贡献 SVG 堆叠柱状图。
//!   纯 std 生成 SVG，无外部绘图依赖。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ablation::AblationReport;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendReport {
    pub iterations: Vec<TrendPoint>,
    pub convergence: Option<ConvergenceAnalysis>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendPoint {
    pub iteration: u32,
    /// 指标名 → 数值。
    pub metrics: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConvergenceAnalysis {
    pub converged: bool,
    /// 首次进入平台期的迭代号。
    pub plateau_at_iteration: u32,
    pub plateau_value: f64,
    /// 进入平台期前每轮平均改进量。
    pub convergence_rate: f64,
}

impl TrendReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, point: TrendPoint) {
        self.iterations.push(point);
    }

    /// 检测某指标的平台期：从某轮起，后续所有取值的最大波动 ≤ tolerance，
    /// 且平台期长度 ≥ min_plateau_len，视为收敛。
    pub fn analyze_convergence(&mut self, metric: &str, tolerance: f64, min_plateau_len: usize) {
        self.convergence = Some(analyze_convergence(
            &self.iterations,
            metric,
            tolerance,
            min_plateau_len,
        ));
    }
}

pub fn analyze_convergence(
    iterations: &[TrendPoint],
    metric: &str,
    tolerance: f64,
    min_plateau_len: usize,
) -> ConvergenceAnalysis {
    let series: Vec<(u32, f64)> = iterations
        .iter()
        .filter_map(|p| p.metrics.get(metric).map(|v| (p.iteration, *v)))
        .collect();
    let not_converged = |series: &[(u32, f64)]| ConvergenceAnalysis {
        converged: false,
        plateau_at_iteration: series.last().map(|(i, _)| *i).unwrap_or(0),
        plateau_value: series.last().map(|(_, v)| *v).unwrap_or(0.0),
        convergence_rate: 0.0,
    };
    if series.len() < min_plateau_len.max(2) {
        return not_converged(&series);
    }
    // 从后往前找最长的尾段，使其内部波动 ≤ tolerance。
    for start in 0..=series.len() - min_plateau_len {
        let tail = &series[start..];
        let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
        for (_, v) in tail {
            lo = lo.min(*v);
            hi = hi.max(*v);
        }
        if hi - lo <= tolerance {
            let (plateau_iter, _) = series[start];
            let plateau_value = tail.iter().map(|(_, v)| *v).sum::<f64>() / tail.len() as f64;
            let first = series[0].1;
            let rate = if plateau_iter > series[0].0 {
                (plateau_value - first) / (plateau_iter - series[0].0) as f64
            } else {
                0.0
            };
            return ConvergenceAnalysis {
                converged: true,
                plateau_at_iteration: plateau_iter,
                plateau_value,
                convergence_rate: rate,
            };
        }
    }
    not_converged(&series)
}

const SVG_WIDTH: u32 = 800;
const SVG_HEIGHT: u32 = 400;
const MARGIN: u32 = 50;

const PALETTE: [&str; 8] = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
];

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// 多指标折线趋势图（SVG）。每个指标一条折线，x 轴为迭代号。
pub fn render_trend_chart(report: &TrendReport) -> Vec<u8> {
    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" font-family="monospace" font-size="12">"##
    ));
    svg.push_str(&format!(
        r##"<rect width="{SVG_WIDTH}" height="{SVG_HEIGHT}" fill="white"/>"##
    ));

    if report.iterations.is_empty() {
        svg.push_str(r##"<text x="400" y="200" text-anchor="middle" fill="#888">no data</text>"##);
        svg.push_str("</svg>");
        return svg.into_bytes();
    }

    // 收集所有指标名（按字母序稳定着色）。
    let mut metric_names: Vec<String> = report
        .iterations
        .iter()
        .flat_map(|p| p.metrics.keys().cloned())
        .collect();
    metric_names.sort();
    metric_names.dedup();

    let (mut x_min, mut x_max) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut y_min, mut y_max) = (f64::INFINITY, f64::NEG_INFINITY);
    for p in &report.iterations {
        x_min = x_min.min(p.iteration as f64);
        x_max = x_max.max(p.iteration as f64);
        for v in p.metrics.values() {
            y_min = y_min.min(*v);
            y_max = y_max.max(*v);
        }
    }
    if (x_max - x_min).abs() < f64::EPSILON {
        x_max = x_min + 1.0;
    }
    if (y_max - y_min).abs() < f64::EPSILON {
        y_max = y_min + 1.0;
    }
    // y 轴留 5% 余量
    let pad = (y_max - y_min) * 0.05;
    y_min -= pad;
    y_max += pad;

    let plot_w = (SVG_WIDTH - 2 * MARGIN) as f64;
    let plot_h = (SVG_HEIGHT - 2 * MARGIN) as f64;
    let fx = |x: f64| MARGIN as f64 + (x - x_min) / (x_max - x_min) * plot_w;
    let fy = |y: f64| (SVG_HEIGHT - MARGIN) as f64 - (y - y_min) / (y_max - y_min) * plot_h;

    // 坐标轴 + 刻度
    svg.push_str(&format!(
        r##"<line x1="{MARGIN}" y1="{}" x2="{}" y2="{}" stroke="#333"/>"##,
        SVG_HEIGHT - MARGIN,
        SVG_WIDTH - MARGIN,
        SVG_HEIGHT - MARGIN
    ));
    svg.push_str(&format!(
        r##"<line x1="{MARGIN}" y1="{MARGIN}" x2="{MARGIN}" y2="{}" stroke="#333"/>"##,
        SVG_HEIGHT - MARGIN
    ));
    for i in 0..=4 {
        let yv = y_min + (y_max - y_min) * i as f64 / 4.0;
        svg.push_str(&format!(
            r##"<text x="{}" y="{:.1}" text-anchor="end" fill="#666">{:.3}</text>"##,
            MARGIN - 6,
            fy(yv) + 4.0,
            yv
        ));
    }

    // 折线
    for (mi, name) in metric_names.iter().enumerate() {
        let color = PALETTE[mi % PALETTE.len()];
        let pts: Vec<String> = report
            .iterations
            .iter()
            .filter_map(|p| {
                p.metrics
                    .get(name)
                    .map(|v| format!("{:.1},{:.1}", fx(p.iteration as f64), fy(*v)))
            })
            .collect();
        if pts.is_empty() {
            continue;
        }
        svg.push_str(&format!(
            r##"<polyline points="{}" fill="none" stroke="{color}" stroke-width="2"/>"##,
            pts.join(" ")
        ));
        // 图例
        let ly = MARGIN + 16 * mi as u32;
        svg.push_str(&format!(
            r##"<rect x="{}" y="{}" width="10" height="10" fill="{color}"/><text x="{}" y="{}" fill="#333">{}</text>"##,
            SVG_WIDTH - MARGIN - 120,
            ly - 9,
            SVG_WIDTH - MARGIN - 106,
            ly,
            esc(name)
        ));
    }

    // 平台期标注
    if let Some(c) = &report.convergence {
        if c.converged {
            let x = fx(c.plateau_at_iteration as f64);
            svg.push_str(&format!(
                r##"<line x1="{x:.1}" y1="{MARGIN}" x2="{x:.1}" y2="{}" stroke="#d62728" stroke-dasharray="4,3"/>"##,
                SVG_HEIGHT - MARGIN
            ));
            svg.push_str(&format!(
                r##"<text x="{:.1}" y="{}" fill="#d62728">plateau @{} (rate {:.4})</text>"##,
                x + 4.0,
                MARGIN - 8,
                c.plateau_at_iteration,
                c.convergence_rate
            ));
        }
    }

    svg.push_str("</svg>");
    svg.into_bytes()
}

/// 消融边际贡献堆叠柱状图（SVG）。每层高度 = 相对前一组的 pass_rate 增量。
pub fn render_ablation_stacked_bar(report: &AblationReport) -> Vec<u8> {
    let stacked = report.delta_stacked_bar();
    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" font-family="monospace" font-size="12">"##
    ));
    svg.push_str(&format!(
        r##"<rect width="{SVG_WIDTH}" height="{SVG_HEIGHT}" fill="white"/>"##
    ));

    if stacked.is_empty() {
        svg.push_str(r##"<text x="400" y="200" text-anchor="middle" fill="#888">no data</text>"##);
        svg.push_str("</svg>");
        return svg.into_bytes();
    }

    let total_max = report
        .groups
        .iter()
        .map(|g| g.pass_rate)
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    let bar_w = 80.0_f64;
    let gap = 30.0_f64;
    let chart_w = stacked.len() as f64 * (bar_w + gap);
    let x0 = (SVG_WIDTH as f64 - chart_w) / 2.0 + gap / 2.0;
    let plot_h = (SVG_HEIGHT - 2 * MARGIN - 40) as f64;
    let baseline_y = (SVG_HEIGHT - MARGIN - 20) as f64;

    svg.push_str(&format!(
        r##"<line x1="{MARGIN}" y1="{baseline_y}" x2="{}" y2="{baseline_y}" stroke="#333"/>"##,
        SVG_WIDTH - MARGIN
    ));

    let mut cum = 0.0;
    for (i, (name, delta)) in stacked.iter().enumerate() {
        let h = (delta.max(0.0) / total_max) * plot_h;
        let x = x0 + i as f64 * (bar_w + gap);
        let y = baseline_y - (cum / total_max) * plot_h - h;
        let color = PALETTE[i % PALETTE.len()];
        svg.push_str(&format!(
            r##"<rect x="{x:.1}" y="{y:.1}" width="{bar_w}" height="{h:.1}" fill="{color}"/>"##
        ));
        svg.push_str(&format!(
            r##"<text x="{:.1}" y="{:.1}" text-anchor="middle" fill="#333">{:+.3}</text>"##,
            x + bar_w / 2.0,
            y - 4.0,
            delta
        ));
        svg.push_str(&format!(
            r##"<text x="{:.1}" y="{:.1}" text-anchor="middle" fill="#333">{}</text>"##,
            x + bar_w / 2.0,
            baseline_y + 16.0,
            esc(name)
        ));
        cum += delta.max(0.0);
    }

    // 显著性标注（* 表示 Welch t-test p < 0.05）
    for (i, d) in report.deltas.iter().enumerate() {
        if let Some((_, p)) = d.significance {
            if p < 0.05 {
                let x = x0 + (i + 1) as f64 * (bar_w + gap) + bar_w / 2.0;
                svg.push_str(&format!(
                    r##"<text x="{x:.1}" y="{:.1}" text-anchor="middle" fill="#d62728" font-size="16">*</text>"##,
                    MARGIN as f64 + 8.0
                ));
            }
        }
    }

    svg.push_str("</svg>");
    svg.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ablation::{AblationDelta, AblationReport, GroupReport};

    fn point(i: u32, v: f64) -> TrendPoint {
        TrendPoint {
            iteration: i,
            metrics: [("pass_rate".to_string(), v)].into_iter().collect(),
        }
    }

    #[test]
    fn detects_plateau() {
        let mut r = TrendReport::new();
        for (i, v) in [
            (0, 0.2),
            (1, 0.4),
            (2, 0.55),
            (3, 0.60),
            (4, 0.605),
            (5, 0.602),
        ] {
            r.push(point(i, v));
        }
        r.analyze_convergence("pass_rate", 0.01, 3);
        let c = r.convergence.unwrap();
        assert!(c.converged);
        assert_eq!(c.plateau_at_iteration, 3);
        assert!(c.convergence_rate > 0.0);
    }

    #[test]
    fn no_plateau_when_still_improving() {
        let mut r = TrendReport::new();
        for i in 0..6 {
            r.push(point(i, 0.1 * i as f64));
        }
        r.analyze_convergence("pass_rate", 0.01, 3);
        assert!(!r.convergence.unwrap().converged);
    }

    #[test]
    fn trend_chart_is_svg() {
        let mut r = TrendReport::new();
        for i in 0..4 {
            r.push(point(i, 0.2 * i as f64));
        }
        let svg = render_trend_chart(&r);
        let s = String::from_utf8(svg).unwrap();
        assert!(s.starts_with("<svg"));
        assert!(s.contains("polyline"));
    }

    #[test]
    fn ablation_bar_is_svg() {
        let report = AblationReport {
            dataset: "toy".into(),
            groups: vec![
                GroupReport {
                    group_name: "Baseline".into(),
                    pass_rate: 0.5,
                    metric_means: HashMap::new(),
                    run_scores: vec![],
                },
                GroupReport {
                    group_name: "+PGE".into(),
                    pass_rate: 0.8,
                    metric_means: HashMap::new(),
                    run_scores: vec![],
                },
            ],
            deltas: vec![AblationDelta {
                group_name: "+PGE".into(),
                pass_rate_delta: 0.3,
                significance: None,
            }],
        };
        let svg = render_ablation_stacked_bar(&report);
        let s = String::from_utf8(svg).unwrap();
        assert!(s.contains("<rect"));
        assert!(s.contains("+PGE"));
    }
}
