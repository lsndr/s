import { posix } from 'node:path';
import type { ReleaseRef } from './release-ref.js';

// fenced blocks (group 1 is the fence) and inline code spans (group 2 is the
// backtick run), both kept verbatim
const CODE =
  /^[ \t]*(`{3,}|~{3,})[^\n]*\n[\s\S]*?(?:^[ \t]*\1[`~]*[ \t]*$|(?![\s\S]))|(`+)(?!`)[\s\S]*?(?<!`)\2(?!`)/gm;

// [text](target "title") and ![alt](target); the text may hold one level of
// brackets so that a linked image like [![a](b)](c) still matches
const INLINE_LINK =
  /(!?)\[((?:[^[\]]|\[[^\]]*\])*)\]\(\s*([^\s)]+)((?:\s+"[^"]*")?)\s*\)/g;

const REFERENCE_DEFINITION = /^( {0,3}\[[^\]]+\]:[ \t]*)(\S+)/gm;

const HAS_SCHEME = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i;

export class ReadmeLinkPinner {
  public constructor(private readonly ref: ReleaseRef) {}

  public pin(markdown: string): string {
    let result = '';
    let last = 0;

    for (const code of markdown.matchAll(CODE)) {
      result += this.pinProse(markdown.slice(last, code.index));
      result += code[0];
      last = code.index + code[0].length;
    }

    return result + this.pinProse(markdown.slice(last));
  }

  private pinProse(prose: string): string {
    return prose
      .replace(
        INLINE_LINK,
        (_match, bang: string, text: string, target: string, title: string) =>
          `${bang}[${this.pinProse(text)}](${this.pinTarget(target, bang === '!')}${title})`,
      )
      .replace(
        REFERENCE_DEFINITION,
        (_match, head: string, target: string) =>
          // a definition doesn't say whether it is used by a link or an image
          `${head}${this.pinTarget(target, false)}`,
      );
  }

  private pinTarget(target: string, isImage: boolean): string {
    if (HAS_SCHEME.test(target)) {
      return target;
    }

    const suffixAt = target.search(/[#?]/);
    const path = suffixAt === -1 ? target : target.slice(0, suffixAt);
    const suffix = suffixAt === -1 ? '' : target.slice(suffixAt);

    const repoPath = path.startsWith('/')
      ? posix.normalize(path.slice(1))
      : posix.join(this.ref.directory, path);

    if (repoPath.startsWith('..')) {
      throw new Error(`The link ${target} leads outside of the repository`);
    }

    const url = isImage
      ? this.ref.rawUrl(repoPath)
      : this.ref.blobUrl(repoPath);

    return url + suffix;
  }
}
