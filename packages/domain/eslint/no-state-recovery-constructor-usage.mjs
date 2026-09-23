import ts from 'typescript';

/** @param {import('typescript').TypeChecker} checker @param {import('typescript').Type} type @returns {boolean} */
function hasStateRecoveryConstructor(checker, type) {
  const symbol = type.getSymbol();
  if (!symbol) return false;

  for (const decl of symbol.declarations ?? []) {
    if (!ts.isClassDeclaration(decl) && !ts.isClassExpression(decl)) continue;

    let hasOwnConstructor = false;

    for (const member of decl.members) {
      if (!ts.isConstructorDeclaration(member)) continue;
      hasOwnConstructor = true;
      if (
        ts.getJSDocTags(member).some((t) => t.tagName.text === 'stateRecovery')
      )
        return true;
      if (
        member.modifiers?.some((m) => m.kind === ts.SyntaxKind.ProtectedKeyword)
      )
        return true;
    }

    // If the class declares its own constructor without @stateRecovery,
    // it overrides the parent — no need to check the hierarchy.
    if (hasOwnConstructor) return false;

    for (const clause of decl.heritageClauses ?? []) {
      if (clause.token !== ts.SyntaxKind.ExtendsKeyword) continue;
      for (const baseTypeNode of clause.types) {
        const baseType = checker.getTypeAtLocation(baseTypeNode.expression);
        if (hasStateRecoveryConstructor(checker, baseType)) return true;
      }
    }
  }

  return false;
}

/** @param {import('typescript').TypeChecker} checker @param {import('typescript').Type} subType @param {import('typescript').Type} superType @returns {boolean} */
function isSameOrSubclass(checker, subType, superType) {
  const subSymbol = subType.getSymbol();
  const superSymbol = superType.getSymbol();
  if (!subSymbol || !superSymbol) return false;
  if (subSymbol === superSymbol) return true;

  for (const decl of subSymbol.declarations ?? []) {
    if (!ts.isClassDeclaration(decl) && !ts.isClassExpression(decl)) continue;
    for (const clause of decl.heritageClauses ?? []) {
      if (clause.token !== ts.SyntaxKind.ExtendsKeyword) continue;
      for (const baseTypeNode of clause.types) {
        const baseType = checker.getTypeAtLocation(baseTypeNode.expression);
        if (isSameOrSubclass(checker, baseType, superType)) return true;
      }
    }
  }

  return false;
}

/** @param {any} services @param {import('typescript').TypeChecker} checker @param {import('@typescript-eslint/types').TSESTree.NewExpression} node @param {import('typescript').Type} instantiatedType @returns {boolean} */
function isInsideSameClassHierarchy(services, checker, node, instantiatedType) {
  let current = node.parent;
  while (current) {
    if (
      current.type === 'ClassDeclaration' ||
      current.type === 'ClassExpression'
    ) {
      const nameNode = current.id;
      if (!nameNode) return false;
      const tsNameNode = services.esTreeNodeToTSNodeMap.get(nameNode);
      if (!tsNameNode) return false;
      const enclosingType = checker.getTypeAtLocation(tsNameNode);
      return isSameOrSubclass(checker, enclosingType, instantiatedType);
    }
    current = current.parent;
  }
  return false;
}

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: 'problem',
    docs: {
      description:
        'Disallow external usage of constructors marked with @stateRecovery JSDoc tag',
    },
    messages: {
      stateRecoveryConstructor:
        'Constructor of "{{className}}" is marked @stateRecovery. ' +
        'Add // eslint-disable-next-line {{ruleId}} if intentional.',
    },
    schema: [],
  },

  create(context) {
    const services = context.sourceCode.parserServices;
    if (!services?.program) return {};

    const checker = services.program.getTypeChecker();

    return {
      NewExpression(node) {
        // new this(...) is always an internal factory call
        if (node.callee.type === 'ThisExpression') return;

        const tsCallee = services.esTreeNodeToTSNodeMap.get(node.callee);
        if (!tsCallee) return;

        const type = checker.getTypeAtLocation(tsCallee);
        if (!hasStateRecoveryConstructor(checker, type)) return;

        if (isInsideSameClassHierarchy(services, checker, node, type)) return;

        context.report({
          node,
          messageId: 'stateRecoveryConstructor',
          data: {
            className:
              node.callee.type === 'Identifier' ? node.callee.name : 'unknown',
            ruleId: context.id,
          },
        });
      },
    };
  },
};
