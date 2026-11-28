# 构建阶段：基于 Node 22 LTS 编译前端
FROM node:22-alpine AS build
WORKDIR /app

# 先装依赖（利用层缓存，仅 package 文件变更时才重装）
COPY package*.json ./
RUN npm ci

# 再拷贝源码并构建
COPY . .
RUN npm run build

# 运行阶段：nginx 只托管静态资源；API 由 Istio VirtualService 直连后端
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
