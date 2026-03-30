import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const settlementLatency = new Trend('settlement_latency');

export const options = {
    stages: [
        { duration: '30s', target: 10 },   // ramp up
        { duration: '1m', target: 50 },    // sustained load
        { duration: '30s', target: 100 },  // peak load
        { duration: '30s', target: 0 },    // ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],  // 95% of requests under 500ms
        errors: ['rate<0.01'],             // error rate under 1%
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
    const eventId = `event-load-${__VU}-${__ITER}`;

    const payload = JSON.stringify({
        eventId: eventId,
        eventName: `Load Test Match ${eventId}`,
        eventWinnerId: `team-${Math.random() > 0.5 ? 'a' : 'b'}`,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(`${BASE_URL}/api/v1/event-outcomes`, payload, params);

    const success = check(res, {
        'status is 202': (r) => r.status === 202,
        'response has eventId': (r) => JSON.parse(r.body).eventId === eventId,
        'response has ACCEPTED status': (r) => JSON.parse(r.body).status === 'ACCEPTED',
    });

    errorRate.add(!success);
    settlementLatency.add(res.timings.duration);

    sleep(0.1);
}

export function handleSummary(data) {
    return {
        stdout: textSummary(data, { indent: ' ', enableColors: true }),
    };
}

function textSummary(data, opts) {
    const metrics = data.metrics;
    return `
K6 Load Test Summary
====================
Total Requests:    ${metrics.http_reqs.values.count}
Success Rate:      ${((1 - metrics.errors.values.rate) * 100).toFixed(2)}%
Avg Duration:      ${metrics.http_req_duration.values.avg.toFixed(2)}ms
P95 Duration:      ${metrics.http_req_duration.values['p(95)'].toFixed(2)}ms
P99 Duration:      ${metrics.http_req_duration.values['p(99)'].toFixed(2)}ms
Max Duration:      ${metrics.http_req_duration.values.max.toFixed(2)}ms
`;
}
