/**
 * ESLint 配置（ESLint 8，使用 .eslintrc 格式）。
 *
 * 只保留能抓"真问题"的规则集，不做格式化风格约束（缩进/引号交给编辑器与 Prettier 习惯）：
 *   - eslint:recommended：JS 层面的未定义变量、不可达代码等
 *   - plugin:vue/vue3-essential：Vue 3 语法与正确性（避免 recommended 带来大量风格噪音）
 *   - @typescript-eslint/recommended：TS 层面的 any 滥用、未使用变量等
 */
module.exports = {
  root: true,
  env: {
    browser: true,
    es2022: true,
    node: true,
  },
  extends: [
    'eslint:recommended',
    'plugin:vue/vue3-essential',
    'plugin:@typescript-eslint/recommended',
  ],
  parser: 'vue-eslint-parser',
  parserOptions: {
    parser: '@typescript-eslint/parser',
    ecmaVersion: 'latest',
    sourceType: 'module',
    extraFileExtensions: ['.vue'],
  },
  ignorePatterns: ['dist', 'node_modules', '*.d.ts'],
  rules: {
    // 后端返回的 DTO 字段可能缺失，代码里存在 `any` 中转；先作为告警而非错误，
    // 避免存量代码一开 lint 就红一片
    '@typescript-eslint/no-explicit-any': 'warn',
    // 允许以 _ 开头的占位参数（事件回调签名需要）
    '@typescript-eslint/no-unused-vars': [
      'error',
      { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
    ],
    // Vue 单文件组件名允许单词（views 下大量单词组件名）
    'vue/multi-word-component-names': 'off',
  },
}
