import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path'

// https://vite.dev/config/
export default defineConfig({
  build: {
    // 生产不输出 source map，避免源码随静态资源一起发布
    sourcemap: false,
  },
  plugins: [vue()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // 用户服务
      '/api/v1/user': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      },
      // 视频服务
      '/api/v1/video': {
        target: 'http://localhost:8082',
        changeOrigin: true,
      },
      // 互动服务
      '/api/v1/interaction': {
        target: 'http://localhost:8083',
        changeOrigin: true,
      },
    },
  },
})
