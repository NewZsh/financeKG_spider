import React, { Component } from 'react';

interface State {
  hasError: boolean;
  error: string;
}

export default class ErrorBoundary extends Component<{ children: React.ReactNode }, State> {
  state: State = { hasError: false, error: '' };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error: error.message || String(error) };
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          maxWidth: 560,
          margin: '80px auto',
          padding: 32,
          borderRadius: 18,
          background: '#fff5f5',
          border: '1px solid #fecaca',
          textAlign: 'center',
        }}>
          <h2 style={{ color: '#b91c1c', marginTop: 0 }}>页面出错了</h2>
          <p style={{ color: '#7f1d1d', lineHeight: 1.7 }}>{this.state.error}</p>
          <button
            onClick={() => {
              this.setState({ hasError: false, error: '' });
              window.location.reload();
            }}
            style={{
              marginTop: 12,
              padding: '10px 22px',
              borderRadius: 8,
              border: 'none',
              background: '#dc2626',
              color: '#fff',
              cursor: 'pointer',
              fontSize: 15,
            }}
          >
            重新加载
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}
